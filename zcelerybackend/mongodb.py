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

    EXTRACTED_FIELDS = [
        "name",
        "series",
        "hostname",
        "start",
        "end",
        "warnings",
        "schedule"
    ]

    def _store_result(self, task_id, result, status,
                      traceback, log, metadata, name,
                      request=None, file_heads=None, 
                      deferred_traceback=None, **kwargs):
        """Store return value and status of an executed task."""
        meta = {'task_id': task_id,
                'status': status,
                'result': result,
                'log': log,
                'metadata': metadata,
                'file_heads':file_heads,
                'traceback': Binary(self.encode(traceback)),
                'deferred_traceback':deferred_traceback,
                'children': Binary(self.encode(
                    self.current_task_children(request),
                ))}
        for field in self.EXTRACTED_FIELDS:
            if field in metadata:
                meta[field] = metadata[field]
                continue
            if "__%s" % field in metadata:
                meta[field] = metadata["__%s" % field]
        self.collection.save(meta)
        return result


    def store_result(self, task_id, result, status,
                         traceback=None, request=None, **kwargs):
        """Update task state and result."""

        def __get(name, default):
            if not result:
                return default
            elif name in result:
               retv = result[name]
               del result[name]
               return retv
            elif hasattr(result, name):
                retv = getattr(result, name)
                delattr(result, name)
                return retv
            else:
                return default

        log = __get("log", [])
        metadata = __get("metadata", {})
        file_heads = __get("file_heads", [])
        deferred_traceback = __get("traceback")

        if "pretty_name" in metadata:
            name = metadata["pretty_name"]
            del metadata["pretty_name"]
        else:
            name = "unknown"

        result = self.encode_result(result, status)
        self._store_result(task_id, result, status, traceback,
                           log, metadata, name,
                           request=request, 
                           file_heads=file_heads,
                           deferred_traceback=deferred_traceback,
                           **kwargs)
        return result

