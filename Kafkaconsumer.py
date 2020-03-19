from kafka import KafkaConsumer
from json import loads
from itertools import chain, starmap


def flatten_json_iterative_solution(dictionary):
    """Flatten a nested json file"""

    def unpack(parent_key, parent_value):
        """Unpack one level of nesting in json file"""
        # Unpack one level only!!!

        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                temp1 = parent_key + '=' + key
                yield temp1, value
        elif isinstance(parent_value, list):
            i = 0
            for value in parent_value:
                temp2 = parent_key + '=' + str(i)
                i += 1
                yield temp2, value
        else:

            yield parent_key, parent_value

            # Keep iterating until the termination condition is satisfied

    while True:
        # Keep unpacking the json file until all values are atomic elements (not dictionary or list)
        dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
        # Terminate condition: not any value in the json file is dictionary or list

        if not any(isinstance(value, dict) for value in dictionary.values()) and \
                not any(isinstance(value, list) for value in dictionary.values()):
            break

    return dictionary


consumer = KafkaConsumer('test',bootstrap_servers=['172.19.218.107:9092']
                         ,auto_offset_reset='earliest'
                         ,enable_auto_commit=True
                         ,value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    message = message.value

    flatten_json_iterative_solution(message)
    """
    print(message["tcpCont"])
    print(message["tcpCont"]["transactionId"])
    print(message["tcpCont"]["svcCode"])
    print(message["tcpCont"]["reqTime"])
"""
