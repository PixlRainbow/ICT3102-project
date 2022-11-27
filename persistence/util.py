from pymongo.collection import Collection

def insert_if_not_exist(im_hash:str, values: dict, collection: Collection):
    collection.update_one(
        {'im_hash':im_hash},
        {'$setOnInsert':values},
        upsert=True
    )

def update_if_older(im_hash: str, qna_json: str, timestamp: int, collection: Collection):
    collection.update_one(
        {
            'im_hash':im_hash,
            'timestamp':{'$lt':timestamp}
        },
        {
            '$set':{
                'qna_list':qna_json,
                'timestamp':timestamp
            }
        }
    )

def get_im(im_hash:str, collection: Collection):
    if im_metadata := collection.find_one({'im_hash':im_hash}):
        return {
            'im_hash': im_metadata['im_hash'],
            'img': im_metadata['img_data'],
            'qnas': im_metadata['qna_list']
        }
    return None
