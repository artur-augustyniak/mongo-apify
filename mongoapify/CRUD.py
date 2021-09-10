import json
import os
import pymongo
from bson import ObjectId
from bson.json_util import dumps
from bson.objectid import ObjectId
from datetime import datetime as dt
import logging

logger = logging.getLogger(__name__)

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, dt):
            return str(o)
        return json.JSONEncoder.default(self, o)


class MongoProvider(object):
    def __init__(self, mongo_url, db, coll, uq_indices=[]):
        self.uq_indices = uq_indices
        self.myclient = pymongo.MongoClient(mongo_url)
        self.mydb = self.myclient[db]
        self.mycol = self.mydb[coll]
        for ui in self.uq_indices:
            self.mycol.create_index(ui, unique=True)

    @staticmethod
    def _create_mongo_sort_dict(sort_list):
        def parse_dir(e):
            if len(e) > 1:
                if e.startswith("+"):
                    return ((e[1:], pymongo.ASCENDING))
                elif e.startswith("-"):
                    return ((e[1:], pymongo.DESCENDING))
                else:
                    return None
            else:
                return None
        lst = list(filter(lambda i: i is not None, map(
            parse_dir,
            sort_list
        )))
        if lst:
            return lst
        else:
            return [('_id', pymongo.DESCENDING), ]

    @staticmethod
    def _create_mongo_filter_dict(filter_list, whole_word, ignore_case, force_and):
        it = iter(filter_list)
        tuples = zip(it, it)

        def make_regex_dicts(t):
            return {t[0]: {'$regex': t[1], '$options': 'i' if ignore_case else ''}}

        def make_eq_dicts(t):
            return {t[0]: {'$regex': '^%s$' % (t[1]), '$options': 'i' if ignore_case else ''}}

        flist = list(map(
            make_eq_dicts if whole_word else make_regex_dicts,
            tuples
        ))
        if flist:
            filter_dict = {"$and" if force_and else "$or" : list(flist)}
            logger.debug("mongo filter %s" % (json.dumps(filter_dict)))
            return filter_dict
        else:
            return {}

    @staticmethod
    def _create_mongo_timerange_filters(created_at_lst, updated_at_lst):
        oplist = ['lte', 'gte', 'eq', 'ne']

        def parse_dt(dt_str):
            try:
                return dt.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                return None

        def make_query_dict(lst):
            query_dict = {}
            it = iter(lst)
            tuples = zip(it, it)
            for t in tuples:
                dt_obj = parse_dt(t[1])
                if t[0] in oplist and dt_obj is not None:
                    query_dict['$%s' % (t[0])] = dt_obj
            return query_dict

        return {
            'created_at': make_query_dict(created_at_lst),
            'updated_at': make_query_dict(updated_at_lst)
        }

    def get_all(self, q={}) -> dict:
        filtering = q.get('filtering', [])
        ignore_case = q.get('ignore_case', False)
        whole_word = q.get('whole_word', True)
        force_and = q.get('force_and', False)
        limit = q.get('limit', 20)
        offset = q.get('offset', 0)
        sort = q.get('sort', ['-_id'])
        created_at = q.get('created_at', [])
        updated_at = q.get('updated_at', [])
        exclude = q.get('exclude', None)
        if exclude is not None:
            exclude = {k: 0 for k in exclude}
        time_fltr = MongoProvider._create_mongo_timerange_filters(
            created_at, updated_at)
        fltr = MongoProvider._create_mongo_filter_dict(
            filtering, whole_word, ignore_case, force_and)
        if time_fltr['created_at']:
            fltr['created_at'] = time_fltr['created_at']
        if time_fltr['updated_at']:
            fltr['updated_at'] = time_fltr['updated_at']
        items = self.mycol.find(fltr, exclude)\
            .skip(offset)\
            .limit(limit)\
            .sort(MongoProvider._create_mongo_sort_dict(sort))
        count = items.count()
        items = list(items)
        items = json.loads(JSONEncoder().encode(items))
        return {
            'count': count,
            'results': items

        }

    def create(self, payload={}):
        pld = payload.get('payload', None)
        if pld is None:
            raise RuntimeError(
                "create - no payload key")
        pld.pop("_id", None)
        creation_time = dt.now()
        pld['created_at'] = creation_time
        pld['updated_at'] = creation_time
        try:
            self.mycol.insert_one(pld)
            return json.loads(JSONEncoder().encode(pld)), False
        except pymongo.errors.DuplicateKeyError as not_uq:
            idx_query = {}
            for k in self.uq_indices:
                idx_query[k] = pld[k] 
            existing = self.mycol.find_one(idx_query)
            return json.loads(JSONEncoder().encode(existing)), True

    def get_one(self, _id) -> dict:
        query = {"_id": ObjectId(_id)}
        item = self.mycol.find_one(query)
        item = JSONEncoder().encode(item)
        return json.loads(item)

    def update(self, _id, upload_payload={}) -> dict:
        query = {"_id": ObjectId(_id)}
        upload_payload.pop("_id", None)
        upload_payload.pop("created_at", None)
        for ui in self.uq_indices:
            upload_payload.pop(ui, None)
        update_time = dt.now()
        upload_payload['updated_at'] = update_time
        new_values = {"$set": upload_payload}
        updated = self.mycol.update_one(query, new_values)
        return self.get_one(_id)

    def delete(self, _id) -> dict:
        query = {"_id": ObjectId(_id)}
        x = self.mycol.delete_one(query)
        if x.deleted_count != 0:
            return {"message": "Item deleted"}
        else:
            None
