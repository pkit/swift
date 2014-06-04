import uuid
import time
from swift.common.swob import Request, HTTPPreconditionFailed, HTTPBadRequest, \
    Response, HTTPNoContent, HTTPCreated, HTTPNotFound
from swift.common.utils import json
from swift.common.wsgi import make_env

COUNTER_DIGITS = 3
ID_DIGITS = 3
TIME_DIGITS = 10

COUNTER_LIMIT = 1 << (COUNTER_DIGITS * 4)
UID_FORMAT = '%%0%dx%%s%%0%dx' % (TIME_DIGITS, COUNTER_DIGITS)


class SortableUid(object):

    def __init__(self):
        self._id = '%s' % uuid.uuid4().hex[:ID_DIGITS]
        self._counter = 0

    def get(self):
        self._counter = (self._counter + 1) % COUNTER_LIMIT
        return UID_FORMAT % ((time.time() * 1000), self._id, self._counter)


class MessageQueue(object):
    def __init__(self, app, conf):
        self.app = app
        self.prefix = conf.get('prefix', '.queue-')
        self._uid_generator = SortableUid()

    def __call__(self, env, start_response):
        req = Request(env)
        try:
            version, account, queue, message = req.split_path(1, 4, True)
        except ValueError:
            return self.app
        if version == 'queue' and account:
            try:
                handler = getattr(self, req.method)
            except AttributeError:
                return HTTPPreconditionFailed(
                    request=req,
                    body='Bad HTTP method')(env, start_response)
            req.path_info_pop()
            container = self.prefix + queue
            resp = handler(req, account, container, message)
            return resp(env, start_response)
        return self.app(env, start_response)

    def POST(self, req, account, container, obj):
        if obj:
            # post message to queue
            if obj != 'message':
                return HTTPBadRequest()
            new_id = self.generate_id()
            req.path_info = '/%s/%s/%s/msg' % (account, container, new_id)
            req.method = 'PUT'
            resp = req.get_response(self.app)
            if resp.status[0] == '2':
                created = json.dumps({'message': {'id': new_id}})
                return HTTPCreated(request=req, body=created)
            return resp
        elif container:
            # update or create queue
            req.method = 'PUT'
            req.path_info = '/%s/%s' % (account, container)
            resp = req.get_response(self.app)
            return resp
        return HTTPBadRequest()

    def GET(self, req, account, container, obj):
        if obj:
            # get specific message
            if self.is_deleted(req, account, container, obj):
                return HTTPNoContent(request=req)
            return self.get_message(req, account, container, obj, None)
        elif container:
            # get any message
            for msg, pending in self.get_message_list(req, account, container):
                if self.is_deleted(req, account, container, msg):
                    continue
                if self.still_pending(req, account, container, pending):
                    continue
                resp = self.get_message(req, account, container, msg, pending)
                if resp.status[0] != '2':
                    continue
                return resp
            return HTTPNoContent(request=req)
        else:
            # get all queues listing
            req.environ['QUERY_STRING'] = 'format=json'
            resp = req.get_response(self.app)
            if resp.status[0] == '2':
                queues = []
                objs = json.loads(resp.body)
                for o in objs:
                    if o.startswith(self.prefix):
                        q = {'name': o['name'].lstrip(self.prefix)}
                        queues.append(q)
                msg = {'queues': queues}
                return Response(request=req, body=json.dumps(msg))
            return resp

    def DELETE(self, req, account, container, obj):
        if obj:
            # delete specific message
            if self.is_deleted(req, account, container, obj):
                return HTTPNotFound(request=req)
            req.method = 'PUT'
            req.path_info = '/%s/%s/%s/deleted' % (account, container, obj)
            resp = req.get_response(self.app)
            return resp
        elif container:
            # delete queue (not supported, for now)
            return HTTPBadRequest()
        return HTTPBadRequest()

    def generate_id(self):
        return self._uid_generator.get()

    def is_deleted(self, req, account, container, obj):
        new_env = make_env(req.environ, method='HEAD', swift_source='queue')
        new_env['PATH_INFO'] = '/%s/%s/%s/deleted' % (account, container, obj)
        new_req = Request(new_env)
        resp = new_req.get_response(self.app)
        if resp.status[0] == '2':
            return True
        if resp.status_int == 404:
            return False
        raise resp

    def get_message_list(self, req, account, container, marker=None):
        data = self.list_container(req, account, container)
        while data:
            deleted = False
            pending = None
            for item in data:
                if item['name'].endswith('/deleted'):
                    deleted = True
                elif item['name'].endswith('/msg'):
                    if not deleted:
                        yield item['name'].rstrip('/msg'), pending
                    deleted = False
                    pending = None
                elif '/' in item['name'] and not deleted:
                    pending = item['name']
            marker = data[-1]['name']
            data = self.list_container(req, account, container, marker=marker)

    def get_message(self, req, account, container, msg, pending):
        if pending:
            new_env = make_env(req.environ, method='GET')
            new_env['PATH_INFO'] = '/%s/%s/%s' % (account, container, pending)
            new_req = Request(new_env)
            resp = new_req.get_response(self.app)
            if resp.status[0] != '2':
                return resp
            data = json.loads(resp.body)
            if time.time() < data['expires']:
                return
        new_env = make_env(req.environ, method='PUT')
        if pending:
            obj = self.increment(pending)
        else:
            obj = self.increment(msg)
        new_env['PATH_INFO'] = '/%s/%s/%s' % (account, container, obj)
        new_req = Request(new_env)


    def list_container(self, req, account, container, marker=None):
        new_env = make_env(req.environ, method='GET',
                           query_string='format=json')
        new_env['PATH_INFO'] = '/%s/%s' % (account, container)
        new_req = Request(new_env)
        if marker:
            new_req.query_string += '&marker=' + marker
        resp = new_req.get_response(self.app)
        if resp.status_int == 204:
            data = resp.body
            return []
        if resp.status_int < 200 or resp.status_int >= 300:
            raise Exception('Error querying object server')
        data = json.loads(resp.body)
        return data

    def still_pending(self, req, account, container, pending):
        pass


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def recon_filter(app):
        return MessageQueue(app, conf)
    return recon_filter