from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['172.19.218.107:9092'],value_serializer=lambda x: dumps(x).encode('utf-8'))

#Note comment
"""
for e in range(1000):
    data = {'number' : e}
    print('loop',e,sep="=")
    producer.send('testods01', value=data)
"""
data = {"svcCont":{"requestObject":{"internalName":{"updateFlag":"New","oldValue":"","value":""},"maxAllowInstance":{"updateFlag":"New","oldValue":"","value":""},"termsAndConditionLink":{"updateFlag":"New","oldValue":"","value":"[]"},"offerStatus":{"updateFlag":"New","oldValue":"","value":""},"validatePeriodUnit":{"updateFlag":"New","oldValue":"","value":""},"offerDescription":{"updateFlag":"New","oldValue":"","value":""},"offerLevel":{"updateFlag":"New","oldValue":"","value":""},"offerEffDate":{"updateFlag":"New","oldValue":"","value":""},"effectiveDate":{"updateFlag":"New","oldValue":"","value":""},"offerName":{"updateFlag":"New","oldValue":"","value":""},"speedInfo":[{"ulSpeedUOM":{"updateFlag":"New","oldValue":"","value":"Mbps"},"speedName":{"updateFlag":"New","oldValue":"","value":"50M50M"},"ulSpeedRate":{"updateFlag":"New","oldValue":"","value":"50.0000"},"dlSpeedRate":{"updateFlag":"New","oldValue":"","value":""},"dlSpeedUOM":{"updateFlag":"New","oldValue":"","value":""}}],"offerPriority":{"updateFlag":"New","oldValue":"","value":""},"startSaleDate":{"updateFlag":"New","oldValue":"","value":""},"independentRelationship":{"updateFlag":"New","oldValue":"","value":""},"offerAttrList":[],"offerRelationshipList":[],"offerType":{"updateFlag":"New","oldValue":"","value":""},"validatePeriod":{"updateFlag":"New","oldValue":"","value":""},"chargeTemplateList":[],"offerAttrGroupList":[],"endSaleDate":{"updateFlag":"New","oldValue":"","value":""},"allowOverrideQuantity":{"updateFlag":"New","oldValue":"","value":""},"offerExpDate":{"updateFlag":"New","oldValue":"","value":""},"offerCompositionList":[{"maximumItemAmount":{"updateFlag":"New","oldValue":"","value":"1"},"itemCode":{"updateFlag":"New","oldValue":"","value":"prod-attr-02"},"compositionType":{"updateFlag":"New","oldValue":"","value":"Product"},"minimumItemAmount":{"updateFlag":"New","oldValue":"","value":"1"}}],"offerTagList":[],"eligibilityRuleList":[],"offerGroupList":[],"offerResourceList":[],"paymentType":{"updateFlag":"New","oldValue":"","value":"[Prepaid]"},"offerID":{"updateFlag":"New","oldValue":"","value":""},"orderControlTemplateList":[],"offerCatalogCode":{"updateFlag":"New","oldValue":"","value":""},"offerCode":{"updateFlag":"New","oldValue":"","value":""},"offerSubType":{"updateFlag":"New","oldValue":"","value":""},"validatePeriodType":{"updateFlag":"New","oldValue":"","value":""},"tariffPlanList":[],"expireDate":{"updateFlag":"New","oldValue":"","value":""},"termsAndCondition":{"updateFlag":"New","oldValue":"","value":""}}},"tcpCont":{"reqTime":"20200301000016","svcCode":"50150000030002","transactionId":"5015000003000120200301000015"}}
producer.send('test', value=data)

producer.flush()
producer.close()