package com.mphrx.core

import com.mphrx.base.BaseService
import grails.converters.JSON
import grails.util.Holders
import org.apache.log4j.Logger
import com.mphrx.dicr.JobConfiguration
import grails.transaction.Transactional
import com.mphrx.util.grails.ApplicationContextUtil

@Transactional
class AthenaMasterIngestionService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaMasterIngestionService");
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaUtil = Holders.grailsApplication.mainContext.getBean("athenaCommonUtilityService");
    def athenaDataIngestionService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;

    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaMasterIngestion";

        if (!changeCurrentJobForMultipleInstance(1, jobConfig.modInstance))
            return

        try {
            log.info("AthenaMasterIngestionService: Job Initiated.")
            callJobProcessing(jobConfig);

        } catch (Exception ex) {
            log.error("AthenaMasterIngestionService.executeService : Exception :", ex);
        } finally {
            log.info("AthenaMasterIngestionService: Job Completed.")
            changeCurrentJobForMultipleInstance(-1, jobConfig.modInstance);
        }
    }

    public void callJobProcessing(JobConfiguration jobConfig) {
        List<String> practiceIds = athenaConf.includePracticeIds;
        List<String> masterApiList = new ArrayList();
        if (jobConfig.multipleBuckets && jobConfig.multipleBuckets != "") {
            masterApiList = jobConfig.multipleBuckets.replaceAll("\\s", "").split(",");
        } else {
            Map masterApis = athenaConst.getAllMasterConfigs();
            masterApiList = masterApis.keySet() as List;
        }

        for (String practiceId in practiceIds) {
            for (String apiName in masterApiList) {
                log.info("AthenaMasterIngestionService: Going to start ingestion for '${apiName}' in Practice '${practiceId}'");
                Map apiConfig = athenaConst.getAPIConfig(apiName);
                Map requestMap =  new LinkedHashMap();
                requestMap.put("apiConfig",apiConfig);
                requestMap.put("practiceId",practiceId);
                Map responseMap = athenaDataIngestionService.routeToAction(athenaConst.INGEST_MASTER_DATA, requestMap);
                if(responseMap.status){
                    log.info("AthenaMasterIngestionService: Ingestion Successful for '${apiName}' in Practice '${practiceId}'");
                } else {
                    log.error("AthenaMasterIngestionService: Ingestion Unsuccessful for '${apiName}' in Practice '${practiceId}'");
                }
            }
        }
        log.info("AthenaMasterIngestionService: End");
    }
}

