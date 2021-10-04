import json
import os
import pymongo
from bson import ObjectId
from bson.json_util import dumps
from bson.objectid import ObjectId
from datetime import datetime as dt
import logging
from .apify import NOT_FOUND_RESP, error_resp, generic_resp

BULK_SIZE = 500

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
                    return (e[1:], pymongo.ASCENDING)
                elif e.startswith("-"):
                    return (e[1:], pymongo.DESCENDING)
                else:
                    return None
            else:
                return None

        lst = list(filter(lambda i: i is not None, map(parse_dir, sort_list)))
        if lst:
            return lst
        else:
            return [
                ("_id", pymongo.DESCENDING),
            ]

    @staticmethod
    def _create_mongo_filter_dict(filter_list, whole_word, ignore_case, force_and):
        it = iter(filter_list)
        tuples = zip(it, it)

        def prepare_tuple(t):
            f, v = t
            try:
                v = json.loads(v)
            except json.decoder.JSONDecodeError:
                pass
            return (f, v)

        def make_query_dict_fun():
            def thunk(t):
                field, val = prepare_tuple(t)
                if type(val) != str:
                    return {field: val}
                else:
                    if whole_word:
                        return {
                            field: {
                                "$regex": "^%s$" % (val),
                                "$options": "i" if ignore_case else "",
                            }
                        }
                    else:
                        return {
                            field: {
                                "$regex": val,
                                "$options": "i" if ignore_case else "",
                            }
                        }

            return thunk

        flist = list(map(make_query_dict_fun(), tuples))
        if flist:
            filter_dict = {"$and" if force_and else "$or": list(flist)}
            logger.debug("mongo filter %s" % (json.dumps(filter_dict)))
            return filter_dict
        else:
            return {}

    @staticmethod
    def _create_mongo_timerange_filters(created_at_lst, updated_at_lst):
        oplist = ["lte", "gte", "eq", "ne"]

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
                    query_dict["$%s" % (t[0])] = dt_obj
            return query_dict

        return {
            "created_at": make_query_dict(created_at_lst),
            "updated_at": make_query_dict(updated_at_lst),
        }

    def get_all(self, q={}) -> dict:
        filtering = q.get("filtering", [])
        ignore_case = q.get("ignore_case", False)
        whole_word = q.get("whole_word", True)
        force_and = q.get("force_and", False)
        shape_mode = q.get("shape_mode", None)
        limit = q.get("limit", 20)
        offset = q.get("offset", 0)
        sort = q.get("sort", ["-_id"])
        created_at = q.get("created_at", [])
        updated_at = q.get("updated_at", [])
        shape = q.get("shape", None)
        if shape is not None:
            shape = {k: int(shape_mode) for k in shape}
        time_fltr = MongoProvider._create_mongo_timerange_filters(
            created_at, updated_at
        )
        fltr = MongoProvider._create_mongo_filter_dict(
            filtering, whole_word, ignore_case, force_and
        )
        if time_fltr["created_at"]:
            fltr["created_at"] = time_fltr["created_at"]
        if time_fltr["updated_at"]:
            fltr["updated_at"] = time_fltr["updated_at"]
        items = (
            self.mycol.find(fltr, shape)
            .skip(offset)
            .limit(limit)
            .sort(MongoProvider._create_mongo_sort_dict(sort))
        )
        count = items.count()
        items = list(items)
        items = json.loads(JSONEncoder().encode(items))
        return {"count": count, "results": items}

    def create(self, payload={}):
        if type(payload) is not dict:
            return (
                error_resp(
                    "create - no payload key or patch_payload is not an object",
                    400,
                ),
                400,
            )
        payload.pop("_id", None)
        creation_time = dt.now()
        payload["created_at"] = creation_time
        payload["updated_at"] = creation_time
        try:
            self.mycol.insert_one(payload)
            return generic_resp("Created", 201), 201
        except pymongo.errors.DuplicateKeyError:
            return error_resp("Item with this key exists", 409), 409

    def get_one(self, _id) -> dict:
        query = {"_id": ObjectId(_id)}
        item = self.mycol.find_one(query)
        item = JSONEncoder().encode(item)
        return json.loads(item)

    def update(self, _id, payload={}) -> dict:
        """
        I don't care about transaction here, if two updates will be that close that they can iterfere
        timestamp will be almost the same
        """
        update_time = dt.now()
        if type(payload) is not dict:
            return (
                error_resp(
                    "update - no patch_payload key or patch_payload is not an object",
                    400,
                ),
                400,
            )

        query = {"_id": ObjectId(_id)}
        payload.pop("_id", None)
        payload.pop("created_at", None)
        for ui in self.uq_indices:
            payload.pop(ui, None)
        new_values = {"$set": payload}
        up_res = self.mycol.update_one(query, new_values)
        if not up_res.acknowledged or up_res.matched_count == 0:
            return NOT_FOUND_RESP, 404

        ret_dict = {}
        ret_dict["patch"] = payload
        if up_res.modified_count == 0:
            return error_resp("Nothing to do", 409), 409

        _ = self.mycol.update_one(query, {"$set": {"updated_at": update_time}})

        return generic_resp("Done", 200), 200

    def delete(self, _id) -> dict:
        query = {"_id": ObjectId(_id)}
        x = self.mycol.delete_one(query)
        if x.deleted_count != 0:
            return {"message": "Item deleted"}
        else:
            None

    def create_bulk(self, payload={}):
        
        if type(payload) is not list:
            raise RuntimeError(
                "create_bulk - no payload key or payload is not an array"
            )
        n_inserted = 0
        sent = len(payload)
        errors = []

        for doc in payload:
            doc.pop("_id", None)
            creation_time = dt.now()
            doc["created_at"] = creation_time
            doc["updated_at"] = creation_time
        try:
            res = self.mycol.insert_many(payload, ordered=False)
            n_inserted = len(res.inserted_ids)
        except pymongo.errors.BulkWriteError as bwe:
            n_inserted = bwe.details.get("nInserted", 0)
            errors = filter(
                lambda i: i is not None,
                map(
                    lambda item: {
                        "key_value": item.get("keyValue", None),
                        "key_pattern": item.get("keyPattern", None),
                        "error_msg": item.get("errmsg", None),
                    },
                    bwe.details.get("writeErrors", []),
                ),
            )

        res = {"sent": sent, "processed": n_inserted, "errors": list(errors)}
        return json.loads(JSONEncoder().encode(res)), n_inserted > 0

    def update_bulk(self, payload={}):
        
        if type(payload) is not list:
            raise RuntimeError(
                "update_bulk - no patch_payload key or payload is not an array"
            )
        sent = len(payload)
        errors = []
        bulk = self.mycol.initialize_unordered_bulk_op()
        counter = 0
        n_matched = 0
        for patch in payload:
            try:
                bulk.find({"_id": ObjectId(patch.get("_id"))}).update(
                    {"$set": patch.get("patch", {})}
                )
                counter += 1
            except Exception as e:
                errors.append(
                    {
                        "key_value": {"_id": patch.get("_id")},
                        "key_pattern": {},
                        "error_msg": str(e),
                    }
                )
            if counter % BULK_SIZE == 0 and counter > 0:
                r_par = bulk.execute()
                n_matched += r_par.get("nMatched", 0)

                bulk = self.mycol.initialize_ordered_bulk_op()

        if counter % BULK_SIZE != 0:
            r_tail = bulk.execute()
            n_matched += r_tail.get("nMatched", 0)
        #TODO ograniczyc jesli nic sie nie zmienilo
        res = {"sent": sent, "processed": n_matched, "errors": list(errors)}
        return json.loads(JSONEncoder().encode(res)), n_matched > 0
