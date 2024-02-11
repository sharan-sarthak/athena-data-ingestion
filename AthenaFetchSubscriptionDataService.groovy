package com.mphrx.core

import com.mphrx.base.BaseService
import grails.util.Holders
import org.apache.log4j.Logger
import com.mphrx.dicr.JobConfiguration
import grails.transaction.Transactional
import com.mongodb.DBObject
import com.mphrx.util.grails.ApplicationContextUtil
import com.mongodb.BasicDBObject
import com.mphrx.commons.consus.MongoService
import org.bson.types.ObjectId

@Transactional
class AthenaFetchSubscriptionDataService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaFetchSubscriptionDataService");
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaUtil = Holders.grailsApplication.mainContext.getBean("athenaCommonUtilityService");
    def athenaDataIngestionService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;
    MongoService mongoService

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaFetchSubscriptionData";

        if (!changeCurrentJobForMultipleInstance(1, jobConfig.modInstance))
            return

        try {
            log.info("AthenaFetchSubscriptionDataService: Job Initiated.")
            callJobProcessing(jobConfig);

        } catch (Exception ex) {
            log.error("AthenaFetchSubscriptionDataService.executeService : Exception :", ex);
        } finally {
            log.info("AthenaFetchSubscriptionDataService: Job Completed.")
            changeCurrentJobForMultipleInstance(-1, jobConfig.modInstance);
        }
    }

    public void callJobProcessing(JobConfiguration jobConfig) {
        List<String> practiceIds = athenaConf.includePracticeIdsForCDC;
        List<String> subscriptionApiToFetch = new ArrayList();
        if (jobConfig.multipleBuckets && jobConfig.multipleBuckets != "") {
            subscriptionApiToFetch = jobConfig.multipleBuckets.replaceAll("\\s", "").split(",");
        } else {
            Map subscriptionApis = athenaConst.getAllSubscriptionAPIs();
            subscriptionApiToFetch = subscriptionApis.keySet() as List;
        }

        outerloop:
        for (String practiceId in practiceIds) {
            log.info("AthenaFetchSubscriptionDataService: Going to start ingestion for Practice: ${practiceId}");

            log.info("AthenaFetchSubscriptionDataService: Going to fetch department list for Practice: ${practiceId}");
            Map apiConfig = athenaConst.getAPIConfig(athenaConst.DEPARTMENT_SEARCH);
            List<DBObject> departmentsObjects = athenaUtil.getAthenaMaster(practiceId, apiConfig);
            if (departmentsObjects.size() > 0) {
                List departmentIds = [];
                Map departmentMap = new LinkedHashMap();
                departmentsObjects.each() { obj ->
                    departmentIds.add(obj.masterId)
                    departmentMap.put(obj.masterId, obj.masterJson);
                }
                log.debug("AthenaFetchSubscriptionDataService: total number of departments: ${departmentIds.size()}");
                List departmentBatches = departmentIds.collate(athenaConf.departmentLimitForSubscriptionApi);
                for (String apiName in subscriptionApiToFetch) {
                    log.info("AthenaFetchSubscriptionDataService: Going to start ingestion for API: ${apiName}");
                    log.debug("Going to divide the departments into batches of '${athenaConf.departmentLimitForSubscriptionApi}' for processing");
                    for(departments in departmentBatches) {
                        Map subsciptionApiRequestMap = [
                                "practiceId"   : practiceId,
                                "practiceid"   : practiceId,
                                "apiName"      : apiName,
                                "departmentid" : departments.join(","),
                                "departmentMap": departmentMap
                        ];
                        log.info("AthenaFetchSubscriptionDataService: Going to fetch data from ${apiName} API for practice: ${practiceId} and departments: ${departments.join(",")} and save data into staging collections.");
                        Map subsciptionApiReponseMap = athenaDataIngestionService.routeToAction(athenaConst.FETCH_SUBSCRIPTION_DATA, subsciptionApiRequestMap);

                        if (athenaUtil.checkJobConfigStatus(jobConfig.id) == false) {
                            log.info("callJobProcessing(): Exiting loop and stopping further processing as AthenaFetchSubscriptionDataService job is disabled.")
                            break outerloop
                        }
                    }
                }
            }
        }
        log.info("AthenaFetchSubscriptionDataService: End");
    }
}

