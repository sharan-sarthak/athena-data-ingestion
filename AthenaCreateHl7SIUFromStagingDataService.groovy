package com.mphrx.core

import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import com.mphrx.util.grails.ApplicationContextUtil
import grails.transaction.Transactional
import grails.util.Holders
import org.apache.log4j.Logger

@Transactional
class AthenaCreateHl7SIUFromStagingDataService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaCreateHl7SIUFromStagingDataService");
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaUtil = Holders.grailsApplication.mainContext.getBean("athenaCommonUtilityService");
    def athenaService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaCreateHl7SIUFromStagingData";

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
        def createHl7ForSourceAndPracticeIdMap = athenaConf.createHl7ForSource
        for(def createHl7ForSourceAndPracticeId : createHl7ForSourceAndPracticeIdMap.entrySet()) {
            if (createHl7ForSourceAndPracticeId?.value?.size() > 0) {
                String subscriptionApi = "appointmentSubscription"

                log.info("callJobProcessing(): Subscription API to create HL7 data - ${subscriptionApi}")
                try {
                    log.info("callJobProcessing(): Fetching reference for ${subscriptionApi} api")
                    Map requestMap = [:]
                    requestMap.put("practiceID", createHl7ForSourceAndPracticeId.key)
                    requestMap.put("sourceOfOrigin", createHl7ForSourceAndPracticeId.value)
                    requestMap.put("apiName", subscriptionApi)
                    requestMap.put("jobInstanceNumber", jobConfig.modInstance)
                    requestMap.put("limit", athenaConf.fetchLimitForHl7)
                    requestMap.put("jobConfigId", jobConfig.id)
                    requestMap.put("hl7Type", "SIU")
                    requestMap.put("eventType", "S12")
                    Map responseMap = athenaService.createReferenceHL7ForPatientLevelData(requestMap)
                    log.info("callJobProcessing(): Response recieved from athena service - ${responseMap}")
                } catch (Exception ex) {
                    log.error("callJobProcessing(): Exception occurred while creating HL7 - ${ex}")
                }
            }else{
                log.info("Skipping for PracticeId : "+createHl7ForSourceAndPracticeId.key)
            }
        }
    }
}
