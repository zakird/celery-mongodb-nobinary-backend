"""
Celery Backend for MongoDB that does not convert task results into a binary
format before storing them in Mongo database.
"""

__copyright__ = "Copyright 2014 Regents of the University of Michigan"
__author__ = "Zakir Durumeric"
__license__ = "Apache License, Version 2.0"
__email__ = "zakird@gmail.com"

from celery.backends.mongodb import MongoBackend, Binary

__all__ = ['MongoNonBinaryBackend']


class MongoNonBinaryBackend(MongoBackend):

    def _store_result(self, task_id, result, status,
                      traceback=None, request=None, **kwargs):
        """Store return value and status of an executed task."""
        meta = {'_id': task_id,
                'status': status,
                'result': result,
                'traceback': Binary(self.encode(traceback)),
                'children': Binary(self.encode(
                    self.current_task_children(request),
                ))}
        # custom handling of data we embed in metadata
        if "log" in result:
            meta["log"] = result["log"]
            del result["log"]
        if "metadata" in result:
            if "__name" in result["metadata"]:
                meta["name"] = result["metadata"]["__name"]
                del result["metadata"]["__name"]
            meta["metadata"] = result["metadata"]
            del result["metadata"]

        self.collection.save(meta)

        return result

