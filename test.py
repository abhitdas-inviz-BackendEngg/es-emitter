import pymongo
from elasticsearch import Elasticsearch


# def insert_ES(doc):
#     try:
#         es = Elasticsearch()
#         resp = es.index(index="tata_luxury_tata_qa2", id=doc['pk'], body=doc)
#     except Exception as e:
#         print("error occured...", e)
#         print("id: " + str(doc['pk']))

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # mongo_url = f"mongodb://{db_config['user']}:{db_config['pass']}@{db_config['host']}:{db_config['port']}/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    mongo_url = "mongodb://test:test1234@insearch-dev.cluster-ckxnylp9tpfb.ap-south-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"

    mongodb = pymongo.MongoClient(mongo_url)
    # mongo_docs = mongodb['search_admin_dev']['index_docs_test'].find(sort=[('last_updated_at', -1)], filter=[('is_valid', True), ('app','luxury')])
    mongo_docs = mongodb['search_admin_dev']['index_docs_test'].find({'app':'luxury', 'is_valid':True}, sort=[('updated_date', -1)])
    # mongo_docs = mongodb['search_admin_dev']['index_docs_test'].find({"Certification-Jewellery-classification_string_mv": {"$exists": True}})
    # all_docs = mongodb.search_admin_dev.index_docs_test.find({"payload.processor-classification_en_string_mv": {"$exists": True}})

    for doc in mongo_docs:
        doc_data = doc['payload']
        try:
            doc_data['processor-classification_en_string_mv']
        except KeyError:
            print("Doesn't exist")
        else:
            print("Exists...")
            print(doc_data['processor-classification_en_string_mv'])
            print(doc_data['pk'])
            print("Next...")



    print("closing cursor...")
    mongodb.close()
    print("END...")
