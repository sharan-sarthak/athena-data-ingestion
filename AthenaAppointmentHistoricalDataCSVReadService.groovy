package com.mphrx.core

import com.mphrx.base.BaseService
import com.mphrx.dicr.JobConfiguration
import com.mphrx.util.grails.ApplicationContextUtil
import grails.transaction.Transactional
import grails.util.Holders
import org.apache.log4j.Logger

@Transactional
class AthenaAppointmentHistoricalDataCSVReadService extends BaseService {

    private static Logger log = Logger.getLogger("com.mphrx.AthenaAppointmentHistoricalDataCSVReadService");
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaService = Holders.grailsApplication.mainContext.getBean("athenaDataIngestionFromApiService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;


    def executeService(JobConfiguration jobConfig) {
        serviceName = "athenaAppointmentHistoricalDataCSVRead";
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
        log.info("Inside AthenaAppointmentHistoricalDataCSVReadService")
        try {
            String subscriptionApi = "appointmentCSVData"
            log.info("callJobProcessing(): Fetching reference for ${subscriptionApi} api")
            Map requestMap = [:]
            requestMap.put("apiName", subscriptionApi)
            requestMap.put("jobInstanceNumber", jobConfig.modInstance)
            requestMap.put("limit", athenaConf.fetchStagingLimitForReference)
            requestMap.put("jobConfigId", jobConfig.id)
            Map responseMap = athenaService.routeToAction(athenaConst.FETCH_HISTORICAL_DATA, requestMap)
            log.info("callJobProcessing(): Response recieved from athena service - ${responseMap}")
        } catch (Exception ex) {
            log.error("callJobProcessing(): Exception occurred while fetching references - ${ex}")
        }
    }


}
