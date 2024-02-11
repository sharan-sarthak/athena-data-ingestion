package com.mphrx.core

import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import com.mphrx.util.grails.ApplicationContextUtil
import grails.transaction.Transactional
import grails.util.Holders
import org.apache.log4j.Logger


@Transactional
class AthenaRDECreationService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaRDECreationService");
    def athenaService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaRDECreation";
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

                Map requestMap = [:]
                requestMap.put("practiceID", createHl7ForSourceAndPracticeId.key)
                requestMap.put("sourceOfOrigin", createHl7ForSourceAndPracticeId.value)
                requestMap.put("jobInstanceNumber", jobConfig.modInstance)
                requestMap.put("jobConfigId", jobConfig.id)
                log.info("Going to create RDE message with PracticeId : "+createHl7ForSourceAndPracticeId.key +" sourceOfOrigin : "+createHl7ForSourceAndPracticeId.value)
                requestMap.put("hl7Type", "RDE")
                requestMap.put("referenceApiList", ["medicationRequestClinicalReference"])
                requestMap.put("eventType", "O17")
                athenaService.createReferenceHL7ForEncounterLevelData(requestMap)
            }else{
                log.info("Skipping for PracticeId : "+createHl7ForSourceAndPracticeId.key)
            }
        }
    }

}