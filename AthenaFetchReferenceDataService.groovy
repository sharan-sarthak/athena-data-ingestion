package com.mphrx.core

import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import com.mphrx.util.grails.ApplicationContextUtil
import grails.transaction.Transactional
import grails.util.Holders
import org.apache.log4j.Logger

@Transactional
class AthenaFetchReferenceDataService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaFetchReferenceDataService");
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaUtil = Holders.grailsApplication.mainContext.getBean("athenaCommonUtilityService");
    def athenaService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaFetchReferenceData";

        if (!changeCurrentJobForMultipleInstance(1, jobConfig.modInstance))
            return

        try {
            log.info("executeService(): Job Initiated.")
            callJobProcessing(jobConfig);
        } catch (Exception ex) {
            log.error("executeService(): Exception :", ex);
        } finally {
            log.info("executeService(): Job Completed.")
            changeCurrentJobForMultipleInstance(-1, jobConfig.modInstance);
        }
    }

    public void callJobProcessing(JobConfiguration jobConfig) {
        Map subscriptionApis = [:]
        List<String> subscriptionApiToFetch = new ArrayList()
        if (jobConfig.multipleBuckets && jobConfig.multipleBuckets != "") {
            subscriptionApiToFetch = jobConfig.multipleBuckets.replaceAll("\\s", "").split(",")
        } else {
            subscriptionApis = athenaConst.getAllSubscriptionAPIs()
            subscriptionApiToFetch = subscriptionApis.keySet() as List
        }
        log.info("callJobProcessing(): Subscription APIs list to fetch reference data - ${subscriptionApiToFetch}")
        for (String subscriptionApi in subscriptionApiToFetch) {
            if(subscriptionApi.equals(athenaConst.PATIENT_SUBSCRIPTION)){

            }else {
                try {
                    log.info("callJobProcessing(): Fetching reference for ${subscriptionApi} api")
                    Map requestMap = [:]
                    requestMap.put("apiName", subscriptionApi)
                    requestMap.put("jobInstanceNumber", jobConfig.modInstance)
                    requestMap.put("limit", athenaConf.fetchStagingLimitForReference)
                    requestMap.put("jobConfigId", jobConfig.id)
                    Map responseMap = athenaService.routeToAction(athenaConst.FETCH_REFERENCE_DATA_FOR_STAGING, requestMap)
                    log.info("callJobProcessing(): Response recieved from athena service - ${responseMap}")
                } catch (Exception ex) {
                    log.error("callJobProcessing(): Exception occurred while fetching references - ${ex}")
                }
            }
        }
    }
}

