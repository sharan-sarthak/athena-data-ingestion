package com.mphrx.core

import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import com.mphrx.util.grails.ApplicationContextUtil
import grails.transaction.Transactional
import grails.util.Holders
import org.apache.log4j.Logger


@Transactional
class AthenaMDMCreationService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaMDMCreationService");
    def athenaIngestionService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaMDMCreation";
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
                requestMap.put("hl7Type", "MDM")
                log.info("Going to create MDM message for documentReferenceClinicalReference with PracticeId : "+createHl7ForSourceAndPracticeId.key +" sourceOfOrigin : "+createHl7ForSourceAndPracticeId.value)
                requestMap.put("eventType", "T02")
                requestMap.put("referenceApiList", ["documentReferenceClinicalReference"])
                athenaIngestionService.createReferenceHL7ForEncounterLevelData(requestMap)

                requestMap = [:];
                requestMap.put("practiceID", createHl7ForSourceAndPracticeId.key)
                requestMap.put("sourceOfOrigin", createHl7ForSourceAndPracticeId.value)
                requestMap.put("jobInstanceNumber", jobConfig.modInstance)
                requestMap.put("jobConfigId", jobConfig.id)
                log.info("Going to create MDM message for clinicalImpressionClinicalReference with PracticeId : "+createHl7ForSourceAndPracticeId.key +" sourceOfOrigin : "+createHl7ForSourceAndPracticeId.value)
                requestMap.put("hl7Type", "MDMCL")
                requestMap.put("eventType", "T02")
                requestMap.put("referenceApiList", ["clinicalImpressionClinicalReference"])
                athenaIngestionService.createReferenceHL7ForPatientLevelData(requestMap)
            }else{
                log.info("Skipping for PracticeId : "+createHl7ForSourceAndPracticeId.key)
            }
        }
    }

}