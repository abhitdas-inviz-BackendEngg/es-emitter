import pymongo
from elasticsearch import Elasticsearch


def insert_ES(doc):
    try:
        # es = Elasticsearch()
        es = Elasticsearch(
            ['http://internal-a4d2eba22727441aeb5d0ce46d51a387-2106664561.ap-south-1.elb.amazonaws.com'],
            http_auth=('elastic', '6BW71xLB61Ejw9j86vX9iZW3'),
            port=80,
        )
        resp = es.index(index="tata_luxury_preprod_data_test", id=doc['code_string'], body=doc)
    except Exception as e:
        print("error occured...", e)
        print("id: " + str(doc['code_string']))

def get_docs():
    print("aaaa")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # mongo_url = f"mongodb://{db_config['user']}:{db_config['pass']}@{db_config['host']}:{db_config['port']}/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
    mongo_url = "mongodb://preprod:PreProd12search@insearch-docdb-preprod.cluster-cx0j3oma32zw.ap-south-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"

    mongodb = pymongo.MongoClient(mongo_url)
    # mongo_docs = mongodb['search_admin_dev']['index_docs_test'].find(sort=[('last_updated_at', -1)], filter=[('is_valid', True), ('app','luxury')])
    mongo_docs = mongodb['insearch-preprod-0']['index_docs_test'].find({'app':'luxury', 'is_valid':True}, sort=[('updated_date', -1)])

    for doc in mongo_docs:
        doc_data = doc['payload']
        insert_ES(doc_data)


    print("closing cursor...")
    mongodb.close()
    print("END...")
