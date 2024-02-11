package com.mphrx.core

import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import com.mphrx.util.grails.ApplicationContextUtil
import grails.transaction.Transactional
import grails.util.Holders
import org.apache.log4j.Logger

@Transactional
class AthenaFetchClinicalReferenceDataService extends BaseService {
    private static Logger log = Logger.getLogger("com.mphrx.AthenaFetchClinicalReferenceDataService");
    def athenaConst = ApplicationContextUtil.getBeanByName("athenaConstantsAndAPIConfigService");
    def athenaUtil = ApplicationContextUtil.getBeanByName("athenaCommonUtilityService");
    def athenaService = ApplicationContextUtil.getBeanByName("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaFetchClinicalReferenceData";

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
        log.info("callJobProcessing(): Fetching clinical data for Eounter")
        Map requestMap = [:]
        requestMap.put("apiName","patientReference")
        requestMap.put("jobInstanceNumber", jobConfig.modInstance)
        requestMap.put("limit", athenaConf.fetchLimitForClinicalReferences)
        requestMap.put("jobConfigId", jobConfig.id)
        requestMap.put("clinicalReferenceApis",athenaConf.fetchClinicalReferenceApisForPatient)
        Map responseMap = athenaService.routeToAction(athenaConst.FETCH_CLINICAL_REFERENCE_DATA, requestMap)
        log.info("callJobProcessing(): Response recieved from athena service for Patient set of API's - ${responseMap}")


        requestMap.put("apiName","encounterReference")
        requestMap.put("clinicalReferenceApis",athenaConf.fetchClinicalReferenceApisForEncounter)
        responseMap = athenaService.routeToAction(athenaConst.FETCH_CLINICAL_REFERENCE_DATA, requestMap)
        log.info("callJobProcessing(): Response recieved from athena service for Encounter set of API's - ${responseMap}")
    }
}