import json
SUCCESSFUL = 0
FAILURE = 1


class ResponseMessage:
    def __init__(self, _id, status, message):
        self.status = status
        self.message = message
        self.id = _id

    def to_json(self):
        return json.dumps(self.__dict__)


class Success(ResponseMessage):
    def __init__(self, _id, message=''):
        ResponseMessage.__init__(self, _id, SUCCESSFUL, message)


class Failure(ResponseMessage):
    def __init__(self, _id, message=''):
        ResponseMessage.__init__(self, _id, FAILURE, message)


class Message:
    def __init__(self, **kwargs):
        self.id = kwargs['id']
        self.file_name = kwargs['file_name']
        self.kind = kwargs['kind']
        self.namespace = kwargs['namespace'] | 'default'

    def to_json(self):
        return json.dumps(self.__dict__)
