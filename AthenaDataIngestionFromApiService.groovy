package com.mphrx.core

import com.mongodb.DBObject
import com.mphrx.consus.hl7.Hl7Message
import com.mphrx.dto.AL1Segment
import com.mphrx.dto.ConditionSegment
import com.mphrx.dto.ObservationSegment
import com.mphrx.dto.ObxSegment
import com.mphrx.dto.PR1Segment
import com.mphrx.dto.ReactionDTO
import com.mphrx.dto.v2.ImmunizationDTO
import com.mphrx.dto.v2.components.AnnotationDTO
import com.mphrx.dto.v2.components.CodeableConceptDTO
import com.mphrx.dto.v2.components.IdentifierDTO
import com.mphrx.dto.v2.components.PeriodDTO
import consus.basetypes.CodeableConcept
import consus.basetypes.HumanNameMetaData
import consus.basetypes.IdentifierMetaData
import consus.basetypes.Period
import consus.basetypes.PractitionerMetaDataV2
import consus.constants.StringConstants
import grails.transaction.Transactional
import com.mphrx.util.grails.ApplicationContextUtil
import grails.util.Holders
import com.mongodb.BasicDBObject
import com.mphrx.dto.GTSegment
import com.mphrx.dto.INSegment
import com.mphrx.commons.consus.MongoService
import org.apache.log4j.Logger
import org.bson.Document
import org.bson.types.ObjectId
import com.mphrx.dto.GoalSegment
import com.mphrx.dto.RASSegment
import com.mphrx.dto.RDESegment
import ca.uhn.hl7v2.DefaultHapiContext;
import ca.uhn.hl7v2.HapiContext;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v251.message.ORU_R01;
import ca.uhn.hl7v2.model.v251.segment.OBR;
import ca.uhn.hl7v2.model.v251.segment.OBX;
import ca.uhn.hl7v2.model.v251.segment.NTE;
import ca.uhn.hl7v2.parser.Parser;
import ca.uhn.hl7v2.model.v251.segment.ORC;
import ca.uhn.hl7v2.model.v251.datatype.CQ;
import ca.uhn.hl7v2.model.v251.datatype.CE;
import ca.uhn.hl7v2.model.v251.datatype.CQ;
import ca.uhn.hl7v2.model.v251.datatype.NM;
import ca.uhn.hl7v2.model.v251.datatype.ED;
import ca.uhn.hl7v2.model.v251.datatype.ST;

@Transactional
class AthenaDataIngestionFromApiService {
    public static Logger log = Logger.getLogger("com.mphrx.AthenaDataIngestionFromApiService")
    //Getting All beans object along with athena specific configs.
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaUtil = Holders.grailsApplication.mainContext.getBean("athenaCommonUtilityService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;
    def pracTypeMapping = ApplicationContextUtil.getConfig().practitioner.typeMapping;
    def athenaPatientAssigningAuthorityIntegratedMapper = ApplicationContextUtil.getConfig().patientAssigningAuthorityIntegratedMapper?.athenaMapper
    MongoService mongoService;

    public Map routeToAction(String action, Map requestMap){
        Map resultMap = new LinkedHashMap();
        log.info("routeToAction()=> action [${action}] triggered.");
        try{
            switch (action)
            {
                case athenaConst.FETCH_SUBSCRIPTION_DATA:
                    return fetchSubscriptionData(requestMap);
                    break;
                case athenaConst.FETCH_REFERENCE_DATA_FOR_STAGING:
                    return fetchReferenceDataForStaging(requestMap);
                    break;
                case athenaConst.CREATE_HL7_DATA_FROM_STAGING:
                    return createHl7DataFromStaging(requestMap);
                    break;
                case athenaConst.FETCH_REFERENCE_DATA:
                    return fetchReferenceData(requestMap);
                    break;
                case athenaConst.FETCH_SEARCH_DATA:
                    return fetchSearchData(requestMap);
                    break;
                case athenaConst.FETCH_CCDA_DATA:
                    return fetchCcdaData(requestMap);
                    break;
                case athenaConst.INGEST_MASTER_DATA:
                    return ingestMasterData(requestMap);
                    break;
                case athenaConst.FETCH_CLINICAL_REFERENCE_DATA:
                    return fetchClinicalReferenceData(requestMap);
                    break;
                case athenaConst.FETCH_HISTORICAL_DATA:
                    return fetchHistoricalData(requestMap);
                    break;
                default:
                    log.error("Received Action [${action}] is not valid. Please check.");
                    break;
            }
        }catch (Exception ex){
            log.error("routeToAction-> action [{$action}] | Exception due to=>", ex);
            resultMap.put("status", athenaConst.FAILURE);
            resultMap.put("message", ex.getMessage());
        }finally{

        }

        return resultMap
    }

    /**
     *
     * @param requestMap : ["apiName": "patientSubscription", "practiceid" : "14352", "195900", "departmentid": "1,2,3,4,5,6,7"]
     * @return
     */
    private Map fetchSubscriptionData(Map requestMap){
        Map responseMap = new LinkedHashMap();
        log.info("Going to fetch the Subscription data for =>"+requestMap.get("apiName"))
        //TODO Need to put a check in source job for any API data missmatch in ApiConfig
        Map apiConfig = athenaConst.getAPIConfig(requestMap.get("apiName"));
        //TODO need to handle totalcount with other exception and avoid infinite loop.
        //TODO need to put the daterange handelling for any unexpected failure and need to fetch the additional data for the same at Job level.
        int totalcount = 1;
        int count = 0;
        int failedRecord = 0;
        while (totalcount > 0) {
            Map resultMap = athenaUtil.athenaCallAPI(apiConfig, requestMap);
            if (resultMap.get("status") == athenaConst.SUCCESS) {
                totalcount = resultMap.get("result").get("totalcount");
                log.info("Received data with Success status. Totalcount : "+resultMap.get("result").get("totalcount"))

                //log.info(resultMap.get("result").get(apiConfig.get("apiResponseKey")).size()+" All data => "+resultMap.get("result").get(apiConfig.get("apiResponseKey")));

                Map additionalInfo = new LinkedHashMap();
                additionalInfo.put("practiceid", requestMap.get("practiceid"));
                additionalInfo.put("apiName", requestMap.get("apiName"));
                resultMap.put("additionalInfo",additionalInfo)
                List dataList = athenaUtil.createStagingDataList(apiConfig, resultMap)

                //log.info(dataList.size()+"All data after conversion => "+dataList);

                dataList.each { data ->
                    //Adding department reference to reduce the API Calls
                    //if(data.get("departmentId"))
                    //    data.put(athenaConst.DEPARTMENT_REFERENCE, requestMap.get("departmentMap").get(data.get("departmentId").toString()));
                    Map saveRes = athenaUtil.saveStagingData(apiConfig, data)
                    if(!saveRes.get("isSuccess")){
                        failedRecord++;
                        log.error("Not able to ingest ${requestMap.get("apiName")}. Data is =>"+data);
                    }
                }
            }else{
                totalcount = 0
                responseMap = resultMap;
                log.error("fetchSubscriptionData(). failure occurred during data Subscription data fetch due to "+resultMap.get("message"));
            }
            if(apiConfig.get("apiStaticParams").get("leaveunprocessed") == "true")
                totalcount = 0
        }

        if(failedRecord > 0){
            responseMap.put("status", athenaConst.FAILURE)
            responseMap.put("failedRecord", failedRecord)
        }else {
            responseMap.put("status", athenaConst.SUCCESS)
        }
        return responseMap;
    }

    /**
     *
     * @param requestMap -: ["apiName":"patientSubscription", "jobInstanceNumber" : 0, "limit": 10]
     * @return
     */
    private Map fetchReferenceDataForStaging(Map requestMap){
        Map responseMap = new LinkedHashMap();
        responseMap.put("status", athenaConst.SUCCESS);
        log.info("Going to fetch the Subscription data for apiName=>"+requestMap.get("apiName")+" jobInstanceNumber=>"+requestMap.get("jobInstanceNumber")+" limit=>"+requestMap.get("limit"))
        int totalcount = 1;
        //TODO Need to handle the infinite loop scenario carefully
        while(totalcount > 0){
            //Creating Search Map
            Map searchMap = createSearchMapForStaging(athenaConst.REFERENCE, requestMap);
            Map result = athenaUtil.fetchAthenaData(searchMap, true, athenaConst.PROCESSING_REFERENCES);
            if(result.get("isSuccess")) {
                log.info("fetchReferenceDataForStaging: Total Received Data: " + result.totalcount)
                totalcount = result.totalcount;

                Map objectLocalCacheMap = new LinkedHashMap(); //TODO Will be used to cache the data to reduce the API HIT for that limit.
                result.objects.each { doc ->
                    //Below code should be converted in seprate function.
                    log.info("Inside objects loop. otherReferenceApis: "+doc.get("otherReferenceApis"));
                    Map updateMap = new LinkedHashMap();
                    List metadataList = new ArrayList(doc.metadataList); //metadataList
                    boolean eligibleForIngest = true;
                    boolean isMemberData = athenaUtil.checkForMemberData(athenaPatientAssigningAuthorityIntegratedMapper?.get(doc?.practiceId)?: doc?.practiceId ,doc?.get(requestMap?.get("apiName"))?.patientid)
                    for(String apiKey : doc.get("otherReferenceApis").keySet()){
                        if (isMemberData || apiKey?.equals(athenaConst.PATIENT_REFERENCE)) {
                            log.info("Inside otherReferenceApis loop.apiKey: "+apiKey);
                            if(doc.get("otherReferenceApis")?.get(apiKey)?.get("value") != null && apiKey != athenaConst.DEPARTMENT_REFERENCE) {
                                Map apiConfig = athenaConst.getAPIConfig(apiKey);
                                Map reqMap = ["departmentid": doc.departmentId, "practiceid": doc.practiceId, "apiName": apiKey, (apiConfig.get("primaryIdKey")): (doc.get("otherReferenceApis")?.get(apiKey)?.get("value")),"ah-practice":"Organization/a-" + doc.departmentId + ".Practice-" + doc.practiceId];

                                Map resultMap = [:];
                                String providerKey = doc.practiceId+"_"+doc.get("otherReferenceApis")?.get(apiKey)?.get("value");
                                if(apiKey == athenaConst.PROVIDER_REFERENCE && doc.get("otherReferenceApis")?.get("providerReference")?.get("value") != null){
                                    athenaUtil.createPractitionerReference(["stagingId":doc._id,"practiceId":doc.practiceId],["provider":doc.get("otherReferenceApis")?.get("providerReference")?.get("value")],apiKey)
                                }else {
                                    resultMap = athenaUtil.athenaCallAPI(apiConfig, reqMap);
                                }
                                if (resultMap.status == athenaConst.SUCCESS) {
                                    //updateMap.put(apiKey, resultMap.get("result"));
                                    metadataList = athenaUtil.createOrUpdateMetadata(["data": resultMap.get("result"), "apiName": apiKey,(apiConfig.primaryIdKey) :(doc.get("otherReferenceApis")?.get(apiKey)?.get("value"))], apiConfig.primaryIdKey, metadataList)
                                    updateMap.put("metadataList", metadataList)
                                    updateMap.put("retries", 0);
                                    //Create record for reference collection
                                    Map createMap = [:]
                                    createMap.put("apiName", apiKey)
                                    createMap.put("departmentId",doc.departmentId)
                                    createMap.put("practiceId",doc.practiceId)
                                    createMap.put("stagingId", doc._id)
                                    createMap.put("jobInstanceNumber", requestMap.jobInstanceNumber)
                                    createMap.put("limit", requestMap.limit)
                                    createMap.put("patientId",doc.get(requestMap.get("apiName"))?.get("patientid"))
                                    createMap.put("encounterId",doc.get(requestMap.get("apiName"))?.get("encounterid"))
                                    createMap.put("isAppointmentSubscription",true)
                                    createMap.put("source","CDC")
                                        createMap.put("isMemberData",isMemberData)
                                    Map resMap = athenaUtil.saveClinicalOrReferenceData(createMap, resultMap.get("result"))
                                    if(!resMap.isSuccess) {
                                        updateMap.put("status", athenaConst.FAILED_REFERENCES);
                                        updateMap.put("retries", doc.retries + 1);
                                        updateMap.put("exceptionMsg", resMap.message);
                                        eligibleForIngest = false;
                                        break;
                                    }
                                }else if (apiKey == athenaConst.PROVIDER_REFERENCE){
                                    continue;
                                }
                                else {
                                    updateMap.put("status", athenaConst.FAILED_REFERENCES);
                                    updateMap.put("retries", doc.retries + 1);
                                    updateMap.put("exceptionMsg", resultMap.message);
                                    eligibleForIngest = false;
                                    break;
                                }
                            }
                        }
                    }
                    //Going to update the response with fetched Object
                    Map updateResult = null
                    if (eligibleForIngest) {
                        updateMap.put("status", athenaConst.PROCESSED)
                        updateMap.put("metadataList", metadataList)
                        updateResult = athenaUtil.updateStagingData(["searchCriteria": ["_id": doc._id], "updateData": updateMap])
                    }else {
                        updateResult = athenaUtil.updateStagingData(["searchCriteria": ["_id": doc._id], "updateData": updateMap])
                    }
                    if(!updateResult.isSuccess){
                        totalcount = 0;
                        log.error("fetchReferenceDataForStaging: => Error while updating the status. Aborting all other message for further processing");
                        responseMap.put("status", athenaConst.FAILURE);
                        responseMap.put("message", "Error while updating the status. Aborting all other message for further processing");
                    }
                }
            }else {
                totalcount = 0;
                log.error("fetchReferenceDataForStaging: => Error while fetching staging data.");
                responseMap.put("status", athenaConst.FAILURE);
                responseMap.put("message", result.get("message"));
            }

            if (athenaUtil.checkJobConfigStatus(requestMap.jobConfigId) == false) {
                log.info("fetchReferenceDataForStaging(): Exiting loop and stopping further processing as AthenaFetchReferenceDataService job is disabled.")
                break
            }
        }
        return responseMap;
    }

    /**
     *
     */
    private Map fetchClinicalReferenceData(Map requestMap){
        Map responseMap = new LinkedHashMap();
        responseMap.put("status", athenaConst.SUCCESS);
        log.info("Going to fetch the Encounter data, jobInstanceNumber=>"+requestMap.get("jobInstanceNumber")+" limit=>"+requestMap.get("limit"))
        int totalcount = 1;
        while(totalcount>0){
            Map searchMap = createSearchMapForReference(athenaConst.CLINICALREFERENCE, requestMap);
            Map result = athenaUtil.fetchAthenaData(searchMap, true, athenaConst.PROCESSING_REFERENCES);
            if(result.get("isSuccess")) {
                log.info("fetchReferenceDataForStaging: Total Received Data: " + result.totalcount)
                totalcount = result.totalcount;
                result.objects.each { doc ->
                    Boolean allDataIngested = true;
                    if(doc.isMemberData) {
                        for(String apiKey : requestMap.get("clinicalReferenceApis")) {
                            if (!doc?.referenceDataFetchedSuccessFully?.contains(apiKey)) {
                                log.info("Going to fetch ${apiKey} for StagingID : "+doc.stagingId+" | patientId : "+doc.patientId)
                                Map apiConfig = athenaConst.getAPIConfig(apiKey);
                                Map reqMap = ["ah-practice": "Organization/a-" + doc.departmentId + ".Practice-" + doc.practiceId, "patient": "a-" + doc.practiceId + ".E-" + doc.patientId,"patientId": new Integer(doc.patientId),"practiceId":doc.practiceId,"departmentId":doc.departmentId] as Map
                                if(requestMap?.apiName?.equals(athenaConst.PATIENT_REFERENCE)){
                                    def department = mongoService.search(new BasicDBObject("masterType","DEPARTMENT").append("practiceId",doc.practiceId),"athenaMaster")?.objects?.getAt(0)
                                    if(department){
                                        reqMap.put("ah-practic","Organization/a-" + department?.masterId + ".Practice-" + doc.practiceId)
                                        reqMap.put("departmentId",department?.masterId)
                                    }
                                }
                                if (requestMap.get("apiName")?.equals("encounterReference")){
                                    reqMap.put("encounter", doc?.resource?.id)
                                    requestMap.put("encounterId", doc?.encounterId)
                                }
                                requestMap.put("stagingId", doc.stagingId);
                                requestMap.put("departmentId", doc.departmentId);
                                requestMap.put("practiceId", doc.practiceId);
                                requestMap.put("patientId",doc.patientId)
                                requestMap.put("apiKey", apiKey);
                                boolean clinicalDataIngested = false
                                while(true) {
                                    Map resultMap = athenaUtil.athenaCallAPI(apiConfig, reqMap);
                                    if (resultMap.status == athenaConst.SUCCESS) {
                                        // Create new record for clininal data
                                        Map resMap = athenaUtil.saveClinicalOrReferenceData(requestMap, resultMap.get("result"));

                                        //Update status of clinicalData to success
                                        if (resMap.isSuccess) {
                                            clinicalDataIngested = true
                                        } else {
                                            allDataIngested = false
                                            clinicalDataIngested = false
                                            break;
                                        }
                                    } else {
                                        if (resultMap.status == athenaConst.FAILURE) {
                                            allDataIngested = false
                                            break;
                                        }
                                    }
                                    String nextUrl = resultMap.get("result").get("link").find{it.relation.equals("next")}?.url
                                    if(nextUrl && nextUrl != ""){
                                        apiConfig.apiEndPoint = "/"+nextUrl.split(".com/")[1];
                                        log.info("Going to fetch next sequence of data with param:  "+ apiConfig.apiEndPoint)
                                        apiConfig.apiParams = [:]
                                        reqMap = [:]
                                        continue;
                                    }else{
                                        break;
                                    }
                                }
                                if (clinicalDataIngested){
                                    if (!doc.referenceDataFetchedSuccessFully) {
                                        doc.referenceDataFetchedSuccessFully = []
                                    }
                                    if (!doc.referenceDataFetchedSuccessFully.contains(apiKey)) {
                                        doc.referenceDataFetchedSuccessFully.add(apiKey)
                                    }
                                    mongoService.update(new BasicDBObject("_id", doc._id), new BasicDBObject("\$set", new BasicDBObject("referenceDataFetchedSuccessFully", doc.referenceDataFetchedSuccessFully)), athenaConst.REFERENCE_COLLECTION);
                                    markStatusfromFetchPendingToPending(doc)
                                }
                            }
                        }
                    }else{
                        allDataIngested = true
                    }
                    if(allDataIngested){
                        //Update status of encounter to success
                        mongoService.update(new BasicDBObject("_id",doc._id),new BasicDBObject("\$set",new BasicDBObject("status", athenaConst.CDF).append("referenceProcessedDate",new Date())),athenaConst.REFERENCE_COLLECTION);
                    }else{
                        //Update status of encounter to fail
                        mongoService.update(new BasicDBObject("_id",doc._id),new BasicDBObject("\$set",new BasicDBObject("status", athenaConst.CDFF).append("retries",doc.retries+1)),athenaConst.REFERENCE_COLLECTION);
                    }
                }
            }
        }
    }

    private void markStatusfromFetchPendingToPending(def doc){
        List referenceDataFetchedSuccessFully = doc.referenceDataFetchedSuccessFully
        if(doc.apiName.equals("patientReference")){
            if(referenceDataFetchedSuccessFully.containsAll(["allergyIntoleranceClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("ADT_ALG")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("ADT_ALG")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","ADT_ALG"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
            if(referenceDataFetchedSuccessFully.containsAll(["conditionProblemClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("PPR")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("PPR")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","PPR"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
            if(referenceDataFetchedSuccessFully.containsAll(["clinicalImpressionClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("MDMCL")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("MDMCL")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","MDMCL"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
            if(referenceDataFetchedSuccessFully.containsAll(["immunizationClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("VXU_NE")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("VXU_NE")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","VXU_NE"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
            if(referenceDataFetchedSuccessFully.containsAll(["diagnosticReportClinicalReference","serviceRequestClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("ORU")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("ORU")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","ORU"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
        }else if(doc.apiName.equals("encounterReference")){
            if(referenceDataFetchedSuccessFully.containsAll(["conditionDiagnosisClinicalReference","observationClinicalReference","procedureClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("ADT")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("ADT")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","ADT"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
            if(referenceDataFetchedSuccessFully.containsAll(["documentReferenceClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("MDM")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("MDM")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","MDM"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }

            if(referenceDataFetchedSuccessFully.containsAll(["medicationRequestClinicalReference"]) && doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("RDE")}?.status?.equals("FETCH_PENDING")){
                doc?.hl7CreationStatusMap?.find{it?.messageType?.equals("RDE")}?.status = "PENDING";
                mongoService.update(new BasicDBObject("_id",doc._id).append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType","RDE"))),new BasicDBObject("\$set",new BasicDBObject("hl7CreationStatusMap.\$.status","PENDING")),"athenaReferenceData")
            }
        }
    }

    public Map createSearchMapForReference(String action, Map requestMap) {
        Map searchMap = new LinkedHashMap();
        BasicDBObject searchCriteria = null;
        BasicDBObject searchFailed = null;
        if (action == athenaConst.CLINICALREFERENCE || action == athenaConst.REFERENCE_HL7 || action == athenaConst.CLINICAL_HL7) {
            if(action == athenaConst.CLINICALREFERENCE) {
                //List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
                //obj.add(new BasicDBObject("status", new BasicDBObject("\$in", [athenaConst.INITIATED, athenaConst.PROCESSING_REFERENCES])));
                //obj.add(new BasicDBObject("status", athenaConst.CDFF).append("retries", new BasicDBObject("\$lt", 3)));
                //searchCriteria = new BasicDBObject("\$or", obj);
                searchCriteria = new BasicDBObject("status",new BasicDBObject("\$in", [athenaConst.INITIATED, athenaConst.PROCESSING_REFERENCES,athenaConst.CDFF])).append("retries", new BasicDBObject("\$lt", 3));
            }
            else if(action == athenaConst.REFERENCE_HL7) {


                searchCriteria = new BasicDBObject("status", new BasicDBObject("\$in", [athenaConst.CDF,athenaConst.CDFF, athenaConst.PROCESSING_REFERENCES]))

                //List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
                //obj.add(new BasicDBObject("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType",requestMap?.messageType).append("status",new BasicDBObject("\$in",[athenaConst.PENDING, athenaConst.PROCESSING])))));
                //obj.add(new BasicDBObject("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType",requestMap?.messageType).append("status",athenaConst.FAILED).append("retries",new BasicDBObject("\$lt",3)))));
                //searchCriteria.append("\$or",obj);
                searchCriteria.append("hl7CreationStatusMap",new BasicDBObject("\$elemMatch",new BasicDBObject("messageType",requestMap?.messageType).append("status",new BasicDBObject("\$in",[athenaConst.PENDING, athenaConst.PROCESSING,athenaConst.FAILED])).append("retries",new BasicDBObject("\$lt",3))))
            }else if(action == athenaConst.CLINICAL_HL7){
                //List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
                //obj.add(new BasicDBObject("status" ,new BasicDBObject("\$in",[athenaConst.INITIATED, athenaConst.PROCESSING_HL7])));
                //obj.add(new BasicDBObject("status", athenaConst.FAILED_HL7).append("retries",new BasicDBObject("\$lt",3)));
                //searchCriteria = new BasicDBObject("\$or",obj);
                searchCriteria = new BasicDBObject("status",new BasicDBObject("\$in",[athenaConst.INITIATED, athenaConst.PROCESSING_HL7, athenaConst.FAILED_HL7])).append("retries",new BasicDBObject("\$lt",3));
                if(requestMap.get("messageType") == "VXU_NE"){
                    searchCriteria.append("encounterId",new BasicDBObject("\$exists",false)).append("resource.occurrenceDateTime",new BasicDBObject("\$gte","2020-01-01"))
                }
            }
            searchCriteria.append("jobInstanceNumber", requestMap.get("jobInstanceNumber"));
            if (requestMap.get("limit") != null) {
                searchMap.put("limit", requestMap.get("limit"));
            }
            if (requestMap.get("encounterId") != null) {
                searchCriteria.append("encounterId", requestMap.get("encounterId"))
            }
            if(requestMap.get("patientId") != null){
                searchCriteria.append("patientId", requestMap.get("patientId"))
            }
            if(requestMap.get("messageType") == "ADT" && requestMap.get("apiName") == athenaConst.OBSERVATION_CLINICALREFERENCE){
                searchCriteria.append("resource.category.coding.code",new BasicDBObject("\$in",["vital-signs","social-history"]))
            }
            if(requestMap.get("messageType") == "PPR" && requestMap.get("apiName") == athenaConst.CONDITION_PROBLEM_CLINICALREFERENCE){
                searchCriteria.append("resource.category.coding.code",new BasicDBObject("\$in",["problem-list-item","health-concern"]))
            }
        }
        searchCriteria.append("apiName", requestMap.get("apiName"))
        if(action == athenaConst.REFERENCE_HL7) {
            searchCriteria.append("practiceId", requestMap.get("practiceID"))
            searchCriteria.append("sourceOfOrigin", new BasicDBObject("\$in", requestMap.get("sourceOfOrigin")))
        }
        if(requestMap.get("stagingId") != null){
            searchCriteria.append("stagingId", requestMap.get("stagingId"))
        }
        searchMap.put("searchCriteria", searchCriteria)
        searchMap.put("offset", 0);
        searchMap.put("sortCriteria", ["_id": 1]);
        searchMap.put("athenaCollection", athenaConst.REFERENCE_COLLECTION)
        if(requestMap?.messageType){
            searchMap.put("messageType",requestMap.messageType)
        }
        return searchMap;
    }

    /**
     *
     * @param requestMap : ["apiName":"patientSubscription", "jobInstanceNumber" : 0, "limit": 10]
     * @return
     */
    private Map createHl7DataFromStaging(Map requestMap){ //required for patientSubscription ADT
        Map responseMap = new LinkedHashMap();
        responseMap.put("status", athenaConst.SUCCESS);
        log.info("createHl7DataFromStaging()=> Going to fetch the Staging data to create HL7 apiName=>"+requestMap.get("apiName")+" jobInstanceNumber=>"+requestMap.get("jobInstanceNumber")+" limit=>"+requestMap.get("limit"))
        int totalcount = 1;
        //TODO Need to handle the infinite loop scenario carefully
        while(totalcount > 0){
            //Creating Search Map
            Map searchMap = createSearchMapForStaging(athenaConst.HL7, requestMap);
            Map result = athenaUtil.fetchAthenaData(searchMap, true, athenaConst.PROCESSING_HL7);
            if(result.get("isSuccess")) {
                log.info("createHl7DataFromStaging: Total Received Data: " + result.totalcount)
                //log.info("createHl7DataFromStaging: result: " + result.objects)
                totalcount = result.totalcount;

                result.objects.each { doc ->
                    //Below code should be converted in seprate function.
                    if (log.isDebugEnabled())
                        log.debug("createHl7DataFromStaging: inside each: " + doc._id)
                    Map updateMap = new LinkedHashMap();
                    List hl7MessageRef = new ArrayList();
                    if(doc.containsKey("hl7MessageRef") && doc.get("hl7MessageRef")?.size() > 0){
                        hl7MessageRef.addAll(doc.get("hl7MessageRef"))
                    }
                    Map hl7Result = createHl7DTO(doc);
                    if(hl7Result.status == athenaConst.SUCCESS && hl7Result.isAllSuccess){
                        hl7MessageRef.addAll(hl7Result.get("hl7Ids"))
                        updateMap.put("hl7MessageRef", hl7MessageRef);
                        updateMap.put("status", athenaConst.PROCESSED_HL7);
                    }else if(!hl7Result.isAllSuccess && hl7Result.get("hl7Ids") && hl7Result.get("hl7Ids")?.size() > 0){
                        hl7MessageRef.addAll(hl7Result.get("hl7Ids"))
                        updateMap.put("hl7MessageRef", hl7MessageRef);
                        updateMap.put("messageType",hl7Result.get("messageType"))
                        updateMap.put("status", athenaConst.PARTIAL_HL7);
                    }
                    else{
                        updateMap.put("status", athenaConst.FAILED_HL7);
                        updateMap.put("retries", doc.retries+1);
                        updateMap.put("exceptionMsg", hl7Result.get("message"));
                    }
                    //Going to update the response with fetched Object
                    Map updateResult = athenaUtil.updateStagingData(["searchCriteria":["_id":doc._id], "updateData": updateMap])
                    if(!updateResult.isSuccess){
                        totalcount = 0;
                        log.error("createHl7DataFromStaging: => Error while updating the status. Aborting all other message for further processing");
                        responseMap.put("status", athenaConst.FAILURE);
                        responseMap.put("message", "Error while updating the status. Aborting all other message for further processing");
                    }
                }
            }else {
                totalcount = 0;
                log.error("createHl7DataFromStaging: => Error while fetching staging data.");
                responseMap.put("status", athenaConst.FAILURE);
                responseMap.put("message", result.get("message"));
            }

            if (athenaUtil.checkJobConfigStatus(requestMap.jobConfigId) == false) {
                log.info("createHl7DataFromStaging(): Exiting loop and stopping further processing as AthenaCreateHl7FromStagingDataService job is disabled.")
                break
            }
        }

        return responseMap;
    }


    /**
     *
     * @param requestMap
     * @return
     */
    private Map fetchReferenceData(Map requestMap){
        Map responseMap = new LinkedHashMap();


        return responseMap;
    }

    /**
     *
     * @param requestMap
     * @return
     */
    private Map fetchSearchData(Map requestMap){
        Map responseMap = new LinkedHashMap();


        return responseMap;
    }

    /**
     *
     * @param requestMap
     * @return
     */
    private Map fetchCcdaData(Map requestMap){
        Map responseMap = new LinkedHashMap();


        return responseMap;
    }

    private Map ingestMasterData(Map requestMap){
        Map responseMap = new LinkedHashMap();
        Map apiSearchRequestMap = ["practiceId": requestMap.practiceId];
        log.info("AthenaMasterIngestionService: Going to fetch department list for Practice: ${requestMap.practiceId}");
        //Getting
        Map apiSearchResp = athenaUtil.athenaCallAPI(requestMap.apiConfig, apiSearchRequestMap);
        Map masterMap = new LinkedHashMap();
//        def responseObject = mongoService.search(new BasicDBObject("masterJson.practiceid",requestMap.practiceId).append("masterType","PRACTICEINFO"),"athenaMaster")
//        ObjectId practiceMongoId = responseObject?.objects?.size()>0 ?responseObject?.objects?.getAt(0)?._id : null
        if (apiSearchResp.get("status").equals(athenaConst.SUCCESS) && apiSearchResp.get("result")
                && apiSearchResp.get("result").get(requestMap.apiConfig.get("apiResponseKey"))) {
            apiSearchResp.get("result").get(requestMap.apiConfig.get("apiResponseKey")).each() { it ->
                masterMap.put(it.get(requestMap.apiConfig.get("primaryIdKey")), it);
            }
        }

        boolean insertionStatus = athenaUtil.createOrUpdateMasterRecords(requestMap.practiceId, masterMap, requestMap.apiConfig);
        responseMap.put("status", insertionStatus);
        return responseMap;
    }


    public Map createSearchMapForStaging(String action, Map requestMap) {
        Map searchMap = new LinkedHashMap();
        BasicDBObject searchCriteria = null;
        BasicDBObject searchFailed = null;
        if (action == athenaConst.REFERENCE) {
            List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
            obj.add(new BasicDBObject("status", new BasicDBObject("\$in", [athenaConst.INITIATED, athenaConst.PROCESSING_REFERENCES])));
            obj.add(new BasicDBObject("status", athenaConst.FAILED).append("retries", new BasicDBObject("\$lt", 3)));
            searchCriteria = new BasicDBObject("\$or", obj);
            searchMap.put("selectedFields", ["apiName": 1, "status": 1, "apiType": 1, "practiceId": 1, "departmentId": 1, "otherReferenceApis": 1, "metadataList": 1, "retries": 1,(requestMap.get("apiName")+".patientid"):1,(requestMap.get("apiName")+".encounterid"):1]);
        } else if (action == athenaConst.HL7) {
            if( (requestMap.get("apiName") instanceof List && (requestMap.get("apiName").contains(athenaConst.APPOINTMENT_SUBSCRIPTION) || requestMap.get("apiName").contains(athenaConst.APPOINTMENT_CSV_DATA))) || (requestMap.get("apiName").equals(athenaConst.APPOINTMENT_SUBSCRIPTION) || requestMap.get("apiName").equals(athenaConst.APPOINTMENT_CSV_DATA))) {
                searchCriteria = new BasicDBObject("status", "PROCESSED").append("referenceHL7Status.status."+requestMap.hl7Type, new BasicDBObject("\$in", ["PENDING", "PROCESSING", "FAILED"])).append("referenceHL7Retries.retries."+requestMap.hl7Type, new BasicDBObject("\$lt", 3));
            }else{
                List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
                obj.add(new BasicDBObject("status", new BasicDBObject("\$in", [athenaConst.INITIATED, athenaConst.PROCESSING_HL7])));
                obj.add(new BasicDBObject("status", athenaConst.FAILED_HL7).append("retries", new BasicDBObject("\$lt", 3)));
                searchCriteria = new BasicDBObject("\$or", obj);
            }
            searchMap.put("selectedFields", [:])
        }
        if (requestMap.get("apiName") != null) {
            searchCriteria.append("apiName", new BasicDBObject("\$in" , requestMap.get("apiName") instanceof List ?requestMap.get("apiName"): [requestMap.get("apiName")]));
        }
        if(action == athenaConst.HL7 && requestMap.get("apiName")== athenaConst.PATIENT_SUBSCRIPTION){
            searchCriteria.append("practiceId",requestMap?.get("practiceID"));
            searchCriteria.append("sourceOfOrigin",new BasicDBObject("\$in", requestMap?.get("sourceOfOrigin")));
        }
        searchCriteria.append("jobInstanceNumber", requestMap.get("jobInstanceNumber"));
        searchMap.put("searchCriteria", searchCriteria)
        searchMap.put("limit", requestMap.get("limit"));
        searchMap.put("offset", 0);
        searchMap.put("sortCriteria", ["_id": 1]);
        searchMap.put("athenaCollection", athenaConst.STAGING_COLLECTION);
        return searchMap;
    }


    /**
     *
     * @param requestMap : Map of Staging collection : athenaStaging
     * @return
     */
    private Map createHl7DTO(Map requestMap){
        log.info("Inside createHl7DTO method with _id - ${requestMap._id} | practiceId - ${requestMap.practiceId} | departmentId - ${requestMap.departmentId}")
        if (log.isDebugEnabled())
            log.debug("Going to create the Hl7 Meesage using Request Map =>"+requestMap);
        Map responseMap = new LinkedHashMap();
        List hl7IdsList = new ArrayList();
        boolean isAllSuccess = true;
        try {
            Map apiConfig = athenaConst.getAPIConfig(requestMap.get("apiName"));
            //Checking patient reference present or not. if not then failing the Job
            String patObjFieldName = (requestMap.get("apiName") == athenaConst.PATIENT_SUBSCRIPTION) ? athenaConst.PATIENT_SUBSCRIPTION : athenaConst.PATIENT_REFERENCE;
            if(!requestMap.containsKey(patObjFieldName)) {
                log.error("Not able to process further as patient data not available");
                responseMap.put("status", athenaConst.FAILURE)
                responseMap.put("message", "Patient information not available.")
                return responseMap;
            }
            //Putting a special handling for encounter diagnosis with ADT
            if(requestMap.get("apiName") == athenaConst.APPOINTMENT_SUBSCRIPTION && requestMap.containsKey(athenaConst.ENCOUNTER_REFERENCE) &&
                    requestMap.get(athenaConst.ENCOUNTER_REFERENCE).get("diagnoses") && requestMap.get(athenaConst.ENCOUNTER_REFERENCE).get("diagnoses")?.size() >0){
                requestMap.get("messageType").put(athenaConst.ADT, athenaConst.ADT_A04);
            }
            requestMap.get("messageType").each { msgType , eventType ->
                log.info("createHl7DTO Loop : Going to create the Hl7 Meesage for messageType =>"+msgType);
                Hl7Message hl7Message = new Hl7Message();
                //Considering patient data is must so creating patient using patient reference / subscription except MFN messages
                hl7Message = createHl7DTOFieldsFromPatientStagingData(requestMap, hl7Message);
                hl7Message.modField = requestMap.get("modField");
                hl7Message.messageType = msgType;
                hl7Message.eventType = eventType;
                hl7Message.processed = athenaConst.PENDING;
                if (msgType.equals("ADT")) {
                    hl7Message.pcpStatus = "ATHENA_SKIPPED";
                    hl7Message.pcpRetries = 0;
                }
                hl7Message.patientType = "N"; //To process the ADT because of no encounter. Need to override by other type of messages
                hl7Message.receivedDate = requestMap.get("dateCreated");
                hl7Message.jobInstanceNumber = athenaUtil.getJobInstanceNumber(requestMap.get("modField"), hl7Message.messageType);

                //DepartmentReference is mandatory for all data set for whom we are creating the HL7 messages
                BasicDBObject locationSearchMap = new BasicDBObject()
                StringJoiner performingLocationId = new StringJoiner("_").add(requestMap.get("practiceId").toString());
                if (requestMap.get("departmentId"))
                    performingLocationId.add(requestMap.get("departmentId").toString());
                locationSearchMap.append("masterId",requestMap.get("departmentId")?.toString())
                locationSearchMap.append("practiceId",requestMap.get("practiceId")?.toString())
                hl7Message.performingLocationId = performingLocationId;
                hl7Message.performingLocationName = mongoService.search(locationSearchMap, "athenaMaster")?.objects?.getAt(0)?.masterJson?.name;
                hl7Message.performingOrganizationId = athenaConf?.metaDataMap?.get(requestMap.get("practiceId"))?.partnerId;
                hl7Message.performingOrganizationName = athenaConf?.metaDataMap?.get(requestMap.get("practiceId"))?.partnerName;
                hl7Message.sendingFacility = athenaConf.sendingFacility
                hl7Message.dateTimeZone=athenaConf.athenaPracticeIds?.get(requestMap.get("practiceId"))?.get("timeZone")
                if (msgType == athenaConst.ADT)
                    hl7Message = createHl7DTOFieldsFromInsuranceAndGuarantor(requestMap, hl7Message)

                if (hl7Message.save(flush: true)) {
                    responseMap.put("status", athenaConst.SUCCESS);
                    hl7IdsList.add(hl7Message.id);
                } else {
                    isAllSuccess = false;
                    responseMap.put("status", athenaConst.FAILURE)
                    responseMap.put("message", "Error occure while saving the object")
                }
            }
        }catch (Exception ex){
            log.error("Exception occurred while create the Hl7Messages instance for request Map - ${requestMap._id} - Exception is =>"+ex.getMessage(), ex)
            responseMap.put("status", athenaConst.FAILURE)
            responseMap.put("message", ex.getMessage())
        }
        responseMap.put("hl7Ids", hl7IdsList);
        responseMap.put("messageType",requestMap.get("messageType"));
        responseMap.put("isAllSuccess", isAllSuccess)
        return responseMap;
    }


    private Hl7Message createHl7DTOFieldsFromPatient(Map patientApiMap , Hl7Message hl7Message){
        //Considering patient is must. it should be present either of subscription or reference
        if(patientApiMap != null) {

            //Creating patient name List
            List<HumanNameMetaData> patientNameList = new ArrayList();
            for(Map name : patientApiMap.get("resource").get("name")){
                Period period = Period.createPeriod(athenaUtil.strToDateFormatTimeZoneConvert(name?.period?.start),athenaUtil.strToDateFormatTimeZoneConvert(name?.period?.end))
                HumanNameMetaData humanNameMetaData = new HumanNameMetaData(name?.get("given")?.getAt(0),name?.get("given")?.getAt(1),name?.get("family"),null,null,null,"Human Name",period)
                patientNameList.add(humanNameMetaData)
            }
            hl7Message.patientNames = patientNameList

            hl7Message.patientNames.sort { patientName1, patientName2 -> patientName1?.period?.startDate?.value <=> patientName2?.period?.startDate?.value }

            //Creating patientIdentifiers
            List patientIdentifierList = new ArrayList();
            Map patientIdentifierMap = new LinkedHashMap();
            String patientId = patientApiMap?.get("resource")?.identifier?.find{it.system.equals("https://fhir.athena.io/sid/ah-patient")}?.get("value")
            patientIdentifierMap.put("id", patientId?.split("-")?.last());
            patientIdentifierMap.put("AA", athenaPatientAssigningAuthorityIntegratedMapper?.get(patientApiMap.get("practiceId"))?: patientApiMap.get("practiceId"));
            patientIdentifierMap.put("TC", "MR");
            patientIdentifierMap.put("useCode", "usual");
            patientIdentifierList.add(patientIdentifierMap);
            hl7Message.patientMRNIds = athenaUtil.createIdentifierDTO(patientIdentifierList);

            //Create patientAlternate Identifiers
            def patientSSN = patientApiMap?.get("resource")?.identifier?.find{it.system.equals("http://hl7.org/fhir/sid/us-ssn")}?.get("value");
            if (patientSSN) {
                patientIdentifierList = new ArrayList();
                patientIdentifierMap = new LinkedHashMap();

                patientIdentifierMap.put("id", patientSSN);
                patientIdentifierMap.put("AA", "SSN");
                patientIdentifierMap.put("TC", "SB");
                patientIdentifierMap.put("useCode", "secondary");
                patientIdentifierList.add(patientIdentifierMap);
                hl7Message.patientAlternateIds = athenaUtil.createIdentifierDTO(patientIdentifierList);
            }

            //Create Address
            List patientAddressList = athenaUtil.getAdressList(patientApiMap?.get("resource")?.address)
            hl7Message.patientAddresses = athenaUtil.createAddressDTO(patientAddressList);

            //Create Telecom
            List patientTelecomList = athenaUtil.getTelecomList(patientApiMap?.get("resource")?.telecom)
            hl7Message.telecomList = athenaUtil.createTelecomListDTO(patientTelecomList);

            //Create Next Of Kinn
            List patientNK1List = new ArrayList();
            Map patientNK1Map = null

            List contactList = patientApiMap.get("resource").get("contact")
            for(Map contact : contactList) {
                if (contact.get("name") && contact.get("relationship")) {
                    patientNK1Map = new LinkedHashMap();
                    patientNK1Map.put("name", contact.get("name")?.get("given")?.getAt(0))
                    patientNK1Map.put("family",contact.get("name")?.get("family"))
                    patientNK1Map.put("relationship", athenaConf?.relationship?.get(contact.get("relationship")?.getAt(0)?.get("text"))?: contact.get("relationship")?.getAt(0)?.get("text"))
                    List telecomList = contact.get("telecom")
                    patientNK1Map.put("homephone", telecomList.find { it.use.equals("home") }?.value)
                    patientNK1Map.put("mobilephone", telecomList.find { it.use.equals("mobile") }?.value)
                    patientNK1Map.put("countrycode", athenaConf.countryCode)  //@TODO have to clean special characters
                    patientNK1List.add(patientNK1Map);
                }
            }

            hl7Message.nk1Segments = athenaUtil.createNK1SegmentDTO(patientNK1List);

            //Generic Field Mapping
            if (patientApiMap.get("resource").get("birthDate")) {
                hl7Message.patientDob = patientApiMap.get("resource").get("birthDate") + "T00:00:00.000+0000";
            }
            if (patientApiMap.get("resource").get("gender") && patientApiMap.get("resource").get("gender") != null) {
                String gender = patientApiMap.get("resource").get("gender");
                if ("male" == gender) {
                    gender = "M";
                } else if ("female" == gender) {
                    gender = "F";
                }
                hl7Message.patientSex = gender;
            }
            if (patientApiMap.get("resource").get("maritalStatus")) {
                hl7Message.patientMaritalStatus = patientApiMap.get("resource").get("maritalStatus").get("coding")?.getAt(0)?.display;
            }
            def patientRaceMap = patientApiMap.get("resource").get("extension").find{it?.url?.equals("http://hl7.org/fhir/us/core/StructureDefinition/us-core-race")}?.get("extension")?.find{it.url.equals("ombCategory")}?.valueCoding
            if (patientRaceMap) {
                hl7Message.patientRace = patientRaceMap?.display;
                hl7Message.patientRaceCode = patientRaceMap?.code
            }
            def patientEthnicityMap = patientApiMap.get("resource").get("extension").find{it?.url?.equals("http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity")}?.get("extension")?.find{it.url.equals("ombCategory")}?.valueCoding
            if (patientEthnicityMap) {
                hl7Message.patientEthnicity = patientRaceMap?.display;
                hl7Message.patientEthnicityCode = patientRaceMap?.code
            }
            def communication = patientApiMap?.get("resource")?.get("communication")?.getAt(0)
            if(communication) {
                hl7Message.preferredLanguageCode = communication?.language?.coding?.getAt(0)?.code
                hl7Message.preferredLanguageText =  communication?.language?.text
            }
            if (patientApiMap.get("resource").get("id")) {
                hl7Message.patientId = patientApiMap.get("resource").get("id")?.split("-")?.last();
            }

            if(patientApiMap.get("resource")?.get("deceasedDateTime") && patientApiMap.get("resource")?.get("deceasedDateTime")!= null){
                hl7Message.patientDeceasedDate = patientApiMap.get("resource")?.get("deceasedDateTime")
                hl7Message.patientDeceasedIndicator = "Y"
            }

            /*
            //TODO need to put the logic with correct date formate for extraction
            try {
                if (patientApiMap.get("deceaseddate")) {
                    hl7Message.patientDeceasedIndicator = "Y";
                    // We are assumming patteren will be "MM/dd/yyyy"
                    String deceaseddateVal = patientApiMap.get("deceaseddate");
                    String[] deceaseddateList = deceaseddateVal.split("/");
                    if (deceaseddateList.length == 3) {
                        deceaseddateVal = deceaseddateList[2] + "-" + deceaseddateList[0] + "-" + deceaseddateList[1] + "T00:00:00.000+0000"
                        hl7Message.patientDeceasedDate = deceaseddateVal;
                    }
                } else {
                    hl7Message.patientDeceasedIndicator = "N";
                }
            }catch (Exception e) {
                log.error("Exception occurred while creating patient deceased date - " + e)
            } */
        }else{
            log.info("No patient information available")
        }

        return hl7Message;
    }

    private Hl7Message createHl7DTOFieldsFromPatientStagingData(Map requestMap, Hl7Message hl7Message){
        //Considering patient is must. it should be present either of subscription or reference
        String patObjFieldName = (requestMap.get("apiName") == athenaConst.PATIENT_SUBSCRIPTION) ? athenaConst.PATIENT_SUBSCRIPTION : athenaConst.PATIENT_REFERENCE;
        if(requestMap.containsKey(patObjFieldName)) {
            Map patientApiMap = requestMap.get(patObjFieldName);

            //Creating patient name List
            List patientNameList = new ArrayList();
            Map patientNameMap = new LinkedHashMap();
            patientNameMap.put("firstname", patientApiMap.get("firstname"));
            patientNameMap.put("lastname", patientApiMap.get("lastname"));
            patientNameMap.put("middlename", patientApiMap.get("middlename"));
            patientNameMap.put("prefix", "");
            patientNameMap.put("suffix", patientApiMap.get("suffix"));

            patientNameList.add(patientNameMap)
            hl7Message.patientNames = athenaUtil.createHumanNameMetaDTO(patientNameList);

            //Creating patientIdentifiers
            List patientIdentifierList = new ArrayList();
            Map patientIdentifierMap = new LinkedHashMap();
            patientIdentifierMap.put("id", patientApiMap.get("patientid"));
            patientIdentifierMap.put("AA", athenaPatientAssigningAuthorityIntegratedMapper?.get(requestMap.get("practiceId"))?:requestMap.get("practiceId"));
            patientIdentifierMap.put("TC", "MR");
            patientIdentifierMap.put("useCode", "usual");
            patientIdentifierList.add(patientIdentifierMap);
            hl7Message.patientMRNIds = athenaUtil.createIdentifierDTO(patientIdentifierList);

            //Create patientAlternate Identifiers
            if (patientApiMap.get("ssn")) {
                patientIdentifierList = new ArrayList();
                patientIdentifierMap = new LinkedHashMap();

                patientIdentifierMap.put("id", patientApiMap.get("ssn"));
                patientIdentifierMap.put("AA", "SSN");
                patientIdentifierMap.put("TC", "SB");
                patientIdentifierMap.put("useCode", "secondary");
                patientIdentifierList.add(patientIdentifierMap);
                hl7Message.patientAlternateIds = athenaUtil.createIdentifierDTO(patientIdentifierList);
            }

            //Create Address
            List patientAddressList = new ArrayList();
            Map patientAddressMap = new LinkedHashMap();
            patientAddressMap.put("address1", patientApiMap.get("address1"));
            patientAddressMap.put("address2", patientApiMap.get("address2"));
            patientAddressMap.put("city", patientApiMap.get("city"));
            patientAddressMap.put("state", patientApiMap.get("state"));
            patientAddressMap.put("zip", patientApiMap.get("zip"));
            patientAddressMap.put("country", patientApiMap.get("countrycode"));
            patientAddressMap.put("useCode", "home");
            patientAddressList.add(patientAddressMap);
            hl7Message.patientAddresses = athenaUtil.createAddressDTO(patientAddressList);

            //Create Telecom
            List patientTelecomList = new ArrayList();
            Map patientTelecomMap = null;
            if (patientApiMap.get("homephone")) {
                patientTelecomMap = new LinkedHashMap();
                patientTelecomMap.put("countryCode", athenaConf.countryCode)
                patientTelecomMap.put("phoneSystemValue", "phone")
                patientTelecomMap.put("phoneUseCode", "home")
                patientTelecomMap.put("phoneValue", patientApiMap.get("homephone"));
                patientTelecomList.add(patientTelecomMap);
            }
            if (patientApiMap.get("mobilephone")) {
                patientTelecomMap = new LinkedHashMap();
                patientTelecomMap.put("countryCode", athenaConf.countryCode)
                patientTelecomMap.put("phoneSystemValue", "phone")
                patientTelecomMap.put("phoneUseCode", "mobile")
                patientTelecomMap.put("phoneValue", patientApiMap.get("mobilephone"));
                patientTelecomList.add(patientTelecomMap);
            }
            if (patientApiMap.get("workphone")) {
                patientTelecomMap = new LinkedHashMap();
                patientTelecomMap.put("countryCode", athenaConf.countryCode)
                patientTelecomMap.put("phoneSystemValue", "phone")
                patientTelecomMap.put("phoneUseCode", "work")
                patientTelecomMap.put("phoneValue", patientApiMap.get("workphone"));
                patientTelecomList.add(patientTelecomMap);
            }
            if (patientApiMap.get("email")) {
                patientTelecomMap = new LinkedHashMap();
                patientTelecomMap.put("phoneSystemValue", "email")
                patientTelecomMap.put("phoneUseCode", "email")
                patientTelecomMap.put("phoneValue", patientApiMap.get("email"));
                patientTelecomList.add(patientTelecomMap);
            }

            hl7Message.telecomList = athenaUtil.createTelecomListDTO(patientTelecomList);

            //Create Next Of Kinn
            List patientNK1List = new ArrayList();
            Map patientNK1Map = null
            if (patientApiMap.get("nextkinname") && patientApiMap.get("nextkinrelationship")) {
                patientNK1Map = new LinkedHashMap();
                patientNK1Map.put("name", patientApiMap.get("nextkinname"))
                patientNK1Map.put("relationship", patientApiMap.get("nextkinrelationship"))
                patientNK1Map.put("homephone", patientApiMap.get("nextkinphone"))
                patientNK1Map.put("countrycode", athenaConf.countryCode)
                patientNK1List.add(patientNK1Map);
            }
            if (patientApiMap.get("contactname") && patientApiMap.get("contactrelationship")) {
                patientNK1Map = new LinkedHashMap();
                patientNK1Map.put("name", patientApiMap.get("contactname"))
                patientNK1Map.put("relationship", patientApiMap.get("contactrelationship"))
                patientNK1Map.put("homephone", patientApiMap.get("contacthomephone"))
                patientNK1Map.put("mobilephone", patientApiMap.get("contactmobilephone"))
                patientNK1Map.put("countrycode", athenaConf.countryCode)
                patientNK1List.add(patientNK1Map);
            }

            hl7Message.nk1Segments = athenaUtil.createNK1SegmentDTO(patientNK1List);

            //Generic Field Mapping
            if (patientApiMap.get("dob")) {
                String dobVal = patientApiMap.get("dob");
                String[] dobList = dobVal?.split("/");
                if (dobList.length == 3) {
                    dobVal = dobList[2] + "-" + dobList[0] + "-" + dobList[1] + "T00:00:00.000+0000"
                    hl7Message.patientDob = dobVal;
                }
            }
            if (patientApiMap.get("sex")) {
                hl7Message.patientSex = patientApiMap.get("sex");
            }
            if (patientApiMap.get("maritalstatus")) {
                hl7Message.patientMaritalStatus = patientApiMap.get("maritalstatus");
            }
            if (patientApiMap.get("racename")) {
                hl7Message.patientRace = patientApiMap.get("racename");
            }
            if (patientApiMap.get("patientid")) {
                hl7Message.patientId = patientApiMap.get("patientid");
            }

            //TODO need to put the logic with correct date formate for extraction
            try {
                if (patientApiMap.get("deceaseddate")) {
                    hl7Message.patientDeceasedIndicator = "Y";
                    // We are assumming patteren will be "MM/dd/yyyy"
                    String deceaseddateVal = patientApiMap.get("deceaseddate");
                    String[] deceaseddateList = deceaseddateVal?.split("/");
                    if (deceaseddateList.length == 3) {
                        deceaseddateVal = deceaseddateList[2] + "-" + deceaseddateList[0] + "-" + deceaseddateList[1] + "T00:00:00.000+0000"
                        hl7Message.patientDeceasedDate = deceaseddateVal;
                    }
                } else {
                    hl7Message.patientDeceasedIndicator = "N";
                }
            }catch (Exception e) {
                log.error("Exception occurred while creating patient deceased date - " + e)
            }
        }else{
            log.info("No patient information available")
        }

        return hl7Message;
    }

    private Hl7Message createHl7DTOFieldsFromAppointment(Map requestMap, Hl7Message hl7Message){
        String objFieldName = (requestMap.get("apiName") == athenaConst.APPOINTMENT_SUBSCRIPTION) ? athenaConst.APPOINTMENT_SUBSCRIPTION : athenaConst.APPOINTMENT_REFERENCE;
        if(requestMap.containsKey(objFieldName)) {
            Map appointmentApiMap = requestMap.get(objFieldName);
            appointmentApiMap.practiceId = requestMap.get("practiceId");
            hl7Message.appointmentCurrentStatus = athenaConst.ATHENA_APPOINTMENT_STATUS.get(appointmentApiMap.get("appointmentstatus"));
            hl7Message.appointmentStatus = athenaConst.ATHENA_APPOINTMENT_STATUS.get(appointmentApiMap.get("appointmentstatus"));
            if(appointmentApiMap.get("appointmentnotes") && appointmentApiMap.get("appointmentnotes")?.size() >0 ){
                appointmentApiMap.get("appointmentnotes").each{ note ->
                    if(!hl7Message.appointmentComment){
                        hl7Message.appointmentComment = note.text
                    }else{
                        hl7Message.appointmentComment = hl7Message.appointmentComment+"<br />"+note.text;
                    }
                }
            }
            hl7Message.appointmentReason = appointmentApiMap.get("cancelreasonname");
            if(appointmentApiMap.get("appointmenttypeid") && appointmentApiMap.get("appointmenttype")){
                hl7Message.appointmentClinicalService = appointmentApiMap.get("appointmenttype");
                List hcsList = [["code":appointmentApiMap.get("appointmenttypeid"),"display":appointmentApiMap.get("appointmenttype"), "locationId": hl7Message.performingLocationId, "locationName": hl7Message.performingLocationName]];
                hl7Message.appointmentType = athenaUtil.createCodeableConceptDTOList(hcsList);
                if(hl7Message.appointmentType.getAt(0).getCoding().getAt(0).system == null){
                    hl7Message.appointmentType.getAt(0).getCoding().getAt(0).system = athenaConf?.metaDataMap?.get(requestMap.get("practiceId"))?.dataSourceId
                }
                hl7Message.healthcareServiceSegments = athenaUtil.createHealthcareServiceSegmentsList(hcsList);
            }
            hl7Message.placerAppointmentId = appointmentApiMap.get("appointmentid").toString();
            hl7Message.fillerAppointmentId = appointmentApiMap.get("appointmentid").toString();

            if(appointmentApiMap.get("date") && appointmentApiMap.get("starttime")) {
                String timeZoneName = requestMap.get("departmentReference")?.get("timezonename") ?: athenaConf.athenaPracticeIds?.get(requestMap.get("practiceId"))?.get("timeZone");
                String appointmentDateStr = appointmentApiMap.get("date")+" "+appointmentApiMap.get("starttime")+":00"; // MM/dd/yyyy HH:mm:ss
                Date appointmentDate = athenaUtil.strToDateFormatTimeZoneConvert(appointmentDateStr, "MM/dd/yyyy HH:mm:ss");
                hl7Message.appointmentStartDateTime = athenaUtil.dateToStrFormatTimeZoneConvert(appointmentDate, "yyyyMMddHHmmss");
                if(appointmentApiMap.get("duration")){
                    hl7Message.appointmentEndDateTime = athenaUtil.dateToStrFormatTimeZoneConvert(athenaUtil.dateAddOperation(appointmentDate,0, Integer.parseInt(appointmentApiMap.get("duration").toString()),0), "yyyyMMddHHmmss");
                }
            }
            //Creating appointmentServiceType
            hl7Message.appointmentServiceType = athenaUtil.createCodeableConceptDTOList([["code":"383", "display":"Consultation"]]);
            if(appointmentApiMap?.providerid) {
                hl7Message.practitionerList.addAll(createHl7DTOFieldsFromPractitioner(appointmentApiMap, StringConstants.PRIMARY_PERFORMER_DOCTOR, ["reference": "Practitioner/"+appointmentApiMap?.providerid]))
            }
        }else{
            log.info("No Appointment information available")
        }
        return hl7Message;
    }


    private Hl7Message createHl7DTOFieldsFromEncounter(Map encounterApiMap, Hl7Message hl7Message,BasicDBObject clinicalDataObject) {
        if(hl7Message?.messageType == "ORU" || hl7Message?.messageType == "ORU_LAB"){
            if(clinicalDataObject != null && clinicalDataObject?.encounterId){
                hl7Message.patientVisitNumber = clinicalDataObject?.encounterId
            }
            else{
                hl7Message.isVisitNumberMandatory = false
            }
        }
        else if (encounterApiMap != null) {

            hl7Message.patientType = athenaConf?.patientClass
            if (null != encounterApiMap.get("resource").get("class")) {
                if (encounterApiMap.get("resource").get("class").code) {
                    hl7Message.patientType = encounterApiMap.get("resource").get("class").code
                } else if (encounterApiMap.get("resource").get("class").display) {
                    hl7Message.patientType = encounterApiMap.get("resource").get("class").display
                }
            }
            hl7Message.patientVisitNumber = encounterApiMap.get("encounterId").toString()
            if(encounterApiMap.get("resource").get("period").get("start")){
                hl7Message.encounterAdmitDate = athenaUtil.strToDateFormatTimeZoneConvert(encounterApiMap.get("resource").get("period").get("start"), null, athenaConf?.timezone)
                if(encounterApiMap.get("resource").get("period").get("end"))
                    hl7Message.encounterDischargeDate = athenaUtil.strToDateFormatTimeZoneConvert(encounterApiMap.get("resource").get("period").get("end"), null, athenaConf?.timezone)
                else if(encounterApiMap.get("resource").get("status") == "finished")
                    hl7Message.encounterDischargeDate = hl7Message.encounterAdmitDate;
            }

            List reasonCodeList = encounterApiMap.get("resource").get("reasonCode")
            if (null != reasonCodeList && !reasonCodeList.isEmpty()) {
                hl7Message.eventReasonDisplay = reasonCodeList[0].text;
                if (null != reasonCodeList[0].coding?.getAt(0)) {
                    if (reasonCodeList[0].coding.getAt(0).code) {
                        hl7Message.eventReasonCode = reasonCodeList[0].coding.getAt(0).code;
                    }
                    if (reasonCodeList[0].coding.getAt(0).system) {
                        hl7Message.eventReasonSystem = reasonCodeList[0].coding.getAt(0).system;
                    }
                }
            }

            List participant = encounterApiMap?.resource?.participant
            if(participant && participant.size() > 0){
                for(Map pracMap : participant){
                    if(pracMap?.individual?.reference?.contains("Practitioner/a")){
                        for(Map type : pracMap?.type){
                            String pracType = type?.coding?.getAt(0)?.code;
                            pracTypeMapping.each{key,value ->
                               if(value == pracType){
                                   pracType = key
                               }
                            }
                            hl7Message.practitionerList.addAll(createHl7DTOFieldsFromPractitioner(encounterApiMap,pracType,participant?.individual))
                        }
                    }
                }
            }
            hl7Message.admissionTypes = athenaUtil.getCodeableConceptDTO(encounterApiMap?.resource?.type)
        }else{
            if(hl7Message.messageType.equals("ADT")){
                hl7Message.isVisitNumberMandatory = false
            }
            log.info("No encounter information available")
        }
        return hl7Message;
    }


    private List<PractitionerMetaDataV2> createHl7DTOFieldsFromPractitioner(def clinicalObject, String practitionerType, def practitionerReference){
        List practitionerList = new LinkedList();
        if(practitionerReference) {
            if (practitionerReference instanceof List) {
                for (def pracReference : practitionerReference) {

                    String[] practitionerIdList = pracReference?.reference?.split("/")
                    String practitionerId;
                    if (practitionerIdList?.size() == 2 && practitionerIdList[0].equals("Practitioner")) {
                        practitionerId = practitionerIdList[1]
                        if(!practitionerId?.contains("-")){
                            practitionerId = "a-"+clinicalObject?.practiceId+".Provider-"+practitionerId
                        }
                    }

                    if (practitionerId) {
                        BasicDBObject practitionerSearchCriteria = new BasicDBObject("practiceId", clinicalObject?.practiceId).append("resource.id", practitionerId).append("apiName", "practitionerReference");
                        Map practitionerMap = mongoService.search(practitionerSearchCriteria, athenaConst.REFERENCE_COLLECTION)
                        if (practitionerMap.totalCount.count > 0) {
                            practitionerList.add(getPractitionerMap(practitionerMap.objects.getAt(0), practitionerType));
                        }
                    }
                }
            } else {
                String[] practitionerIdList = practitionerReference?.reference?.split("/")
                String practitionerId;
                if (practitionerIdList?.size() == 2 && practitionerIdList[0].equals("Practitioner")) {
                    practitionerId = practitionerIdList[1]
                    if(!practitionerId?.contains("-")){
                        practitionerId = "a-"+clinicalObject?.practiceId+".Provider-"+practitionerId
                    }
                }
                if (practitionerId) {
                    BasicDBObject practitionerSearchCriteria = new BasicDBObject("practiceId", clinicalObject?.practiceId).append("resource.id", practitionerId).append("apiName", "practitionerReference")
                    Map practitionerMap = mongoService.search(practitionerSearchCriteria, athenaConst.REFERENCE_COLLECTION)
                    if (practitionerMap.totalCount.count > 0) {
                        practitionerList.add(getPractitionerMap(practitionerMap.objects.getAt(0), practitionerType));
                    }
                }
            }
            return athenaUtil.createPractitionerDTOList(practitionerList);
        }else{
            return null;
        }


    }

    public Map getPractitionerMap(def practitioner, String practitionerType){

        def practitionerResource = practitioner?.resource
        Map practitionerMap = new LinkedHashMap();
        practitionerMap.put("firstname", practitionerResource?.get("name")?.getAt(0)?.get("given")?.getAt(0));
        practitionerMap.put("lastname", practitionerResource?.get("name")?.getAt(0)?.get("family"));
        practitionerMap.put("middlename", practitionerResource?.get("name")?.getAt(0)?.get("given")?.size()>1 ? practitionerResource?.get("name")?.getAt(0)?.get("given")?.getAt(1): null);
        practitionerMap.put("prefix", "");
        practitionerMap.put("suffix", "");
        //String identifier = practitionerResource?.identifier?.find{it?.system?.equals("https://directory.athena.io/sid/provider")}?.value?.toString();
        //practitionerMap.put("identifier", identifier?.split("\\.")?.last());
        //practitionerMap.put("AA", "https://directory.athena.io/sid/provider");
        //practitionerMap.put("TC", "MD");
        List identifier = []
        for(def identifierData : practitionerResource?.identifier){
            if(!identifierData?.type?.text?.equals("NPI")){
                identifier.add(identifierData)
            }
        }
        practitionerMap.put("identifier",identifier)
        practitionerMap.put("practitionerType", practitionerType);
        practitionerMap.put("useCode", practitionerResource?.get("name")?.getAt(0)?.get("use")?: "usual");
        practitionerMap.put("gender", practitionerResource?.get("sex"));
        practitionerMap.put("npi", practitionerResource?.identifier?.find{it?.system?.equals("http://hl7.org/fhir/sid/us-npi")}?.value);
        practitionerMap.put("addressList",practitionerResource?.get("address"))
        practitionerMap.put("telecomList",practitionerResource?.get("telecom"))
        return practitionerMap;
    }

    private Hl7Message createHl7DTOFieldsFromInsuranceAndGuarantor(Map requestMap, Hl7Message hl7Message){
        if (log.isDebugEnabled())
            log.debug("createHl7DTOFieldsFromInsuranceAndGuarantor() : requestMap received is -> ${requestMap._id}")
        String objFieldName = (requestMap.get("apiName") == athenaConst.PATIENT_SUBSCRIPTION) ? athenaConst.PATIENT_SUBSCRIPTION : athenaConst.PATIENT_REFERENCE;
        if(requestMap.containsKey(objFieldName)) {
            Map patientData = requestMap.get(objFieldName);

            //Creating Guarantor segment
            if (!hl7Message.gtSegments) {
                hl7Message.gtSegments = new ArrayList<GTSegment>();
            }
            GTSegment guarantorSeg = new GTSegment();

            if (patientData.get("guarantorssn")) {
                List guarantorSsn = [["identifier": patientData.get("guarantorssn"), "useCode": "usual", "AA": "SSN", "TC": "SB"]]
                //guarantorSeg.ssn = athenaUtil.createIdentifierDTOList(guarantorSsn)
                guarantorSeg.guarantorNumbers = athenaUtil.createIdentifierDTOList(guarantorSsn)
                //Using ssn as guarantorIdentifier for now, need to remove once we have mapping for guarantor identifier/number
            }

            List guarantorTelecomList = new ArrayList();
            Map guarantorTelecom = null
            if (patientData.get("guarantorphone")) {
                guarantorTelecom = new LinkedHashMap()
                guarantorTelecom.put("countryCode", athenaConf.countryCode)
                guarantorTelecom.put("value", patientData.get("guarantorphone"))
                guarantorTelecom.put("system", "phone")
                guarantorTelecom.put("useCode", "home")
                guarantorTelecomList.add(guarantorTelecom)
            }
            if (patientData.get("guarantoremail")) {
                guarantorTelecom = new LinkedHashMap()
                guarantorTelecom.put("value", patientData.get("guarantoremail"))
                guarantorTelecom.put("system", "email")
                guarantorTelecom.put("useCode", "email")
                guarantorTelecomList.add(guarantorTelecom)
            }
            guarantorSeg.phones = athenaUtil.createTelecomDTO(guarantorTelecomList)

            Map guarantorAddress = new LinkedHashMap()
            guarantorAddress.put("line1", patientData.get("guarantoraddress1"))
            guarantorAddress.put("line2", patientData.get("guarantoraddress2"))
            guarantorAddress.put("city", patientData.get("guarantorcity"))
            guarantorAddress.put("state", patientData.get("guarantorstate"))
            guarantorAddress.put("zip", patientData.get("guarantorzip"))
            guarantorAddress.put("country", patientData.get("guarantorcountrycode"))
            guarantorSeg.addresses = athenaUtil.createAddressDTOList([guarantorAddress])

            if (patientData.get("guarantordob"))
                guarantorSeg.birthDate = athenaUtil.strToDateFormatTimeZoneConvert(patientData.get("guarantordob"), "MM/dd/yyyy")
            if (patientData.get("guarantorfirstname")) {
                List guarantorName = [["firstname": patientData.get("guarantorfirstname"), "lastname": patientData.get("guarantormiddlename"), "middlename": "", "prefix": "", "suffix": patientData.get("guarantorsuffix")]]
                guarantorSeg.names = athenaUtil.createHumanNameDTO(guarantorName)
            }
            if (patientData.get("guarantorrelationshiptopatient") && athenaConst.RELATIONSHIP_MAPPING.containsKey(patientData.get("guarantorrelationshiptopatient").toString()))
                guarantorSeg.relationship = athenaConst.RELATIONSHIP_MAPPING.get(patientData.get("guarantorrelationshiptopatient").toString())

            //Employer details
            if (patientData.get("employerNames")) {
                List employerName = [["firstname": patientData.get("employerNames"), "lastname": "", "middlename": "", "prefix": "", "suffix": ""]]
                guarantorSeg.employerNames = athenaUtil.createHumanNameDTO(employerName)
            }
            if (patientData.get("employerPhones")) {
                Map employerTelecomMap = new LinkedHashMap()
                employerTelecomMap.put("countryCode", athenaConf.countryCode)
                employerTelecomMap.put("value", patientData.get("employerPhones"))
                employerTelecomMap.put("system", "phone")
                employerTelecomMap.put("useCode", "home")
                guarantorSeg.employerPhones = athenaUtil.createTelecomDTO([employerTelecomMap])
            }
            Map employerAddress = new LinkedHashMap()
            employerAddress.put("line1", patientData.get("employerAddresses"))
            employerAddress.put("city", patientData.get("employercity"))
            employerAddress.put("state", patientData.get("employerstate"))
            employerAddress.put("zip", patientData.get("employerzip"))
            guarantorSeg.employerAddresses = athenaUtil.createAddressDTOList([employerAddress])

            hl7Message.gtSegments.add(guarantorSeg)

            //Creating Insurance segment
            if (patientData.get("insurances")) {
                if(!hl7Message.inSegments){
                    hl7Message.inSegments = new ArrayList<INSegment>();
                }
                patientData.get("insurances").each { insurance ->
                    INSegment insuranceSeg = new INSegment()

                    if (insurance.get("insurancepolicyholderfirstname") || insurance.get("insurancepolicyholderlastname")) {
                        Map InsuredName = new LinkedHashMap()
                        InsuredName.put("firstname", insurance.get("insurancepolicyholderfirstname"))
                        InsuredName.put("lastname", insurance.get("insurancepolicyholderlastname"))
                        InsuredName.put("middlename", insurance.get("insurancepolicyholdermiddlename"))
                        InsuredName.put("suffix", insurance.get("insurancepolicyholdersuffix"))
                        insuranceSeg.nameOfInsured = athenaUtil.createHumanNameDTO([InsuredName]).getAt(0)
                    }

                    Map insuredAddress = new LinkedHashMap()
                    insuredAddress.put("line1", insurance.get("insurancepolicyholderaddress1"))
                    insuredAddress.put("line2", insurance.get("insurancepolicyholderaddress2"))
                    insuredAddress.put("city", insurance.get("insurancepolicyholdercity"))
                    insuredAddress.put("state", insurance.get("insurancepolicyholderstate"))
                    insuredAddress.put("zip", insurance.get("insurancepolicyholderzip"))
                    insuredAddress.put("country", insurance.get("insurancepolicyholdercountrycode"))
                    insuranceSeg.insuredAddresses = athenaUtil.createAddressDTOList([insuredAddress])

                    if (insurance.get("insurancepolicyholderdob"))
                        insuranceSeg.insuredDateOfBirth = athenaUtil.strToDateFormatTimeZoneConvert(insurance.get("insurancepolicyholderdob"), "MM/dd/yyyy") //Assuming the format to be MM/dd/yyyy as we don't have sample data
                    if (insurance.get("relationshiptoinsuredid") && athenaConst.RELATIONSHIP_MAPPING.containsKey(insurance.get("relationshiptoinsuredid").toString()))
                        insuranceSeg.insuredRelationshipsToPatient = athenaUtil.createCodeableConceptDTOList([["code": insurance.get("relationshiptoinsuredid").toString(), "display": athenaConst.RELATIONSHIP_MAPPING.get(insurance.get("relationshiptoinsuredid").toString())]])
                    else if (insurance.get("relationshiptoinsured"))
                        insuranceSeg.insuredRelationshipsToPatient = athenaUtil.createCodeableConceptDTOList([["display": insurance.get("relationshiptoinsured")]])
                    if (insurance.get("issuedate"))
                        insuranceSeg.coverageStartDate = athenaUtil.strToDateFormatTimeZoneConvert(insurance.get("issuedate"), "MM/dd/yyyy", athenaConf?.timezone)
                    if (insurance.get("expirationdate"))
                        insuranceSeg.coverageEndDate = athenaUtil.strToDateFormatTimeZoneConvert(insurance.get("expirationdate"), "MM/dd/yyyy", athenaConf?.timezone)

                    if (insurance.get("insurancepolicyholderssn"))
                        insuranceSeg.insuredIDNumber = insurance.get("insurancepolicyholderssn")
                    if (insurance.get("insurancepolicyholdersex"))
                        insuranceSeg.insuredAdministrativeSex = insurance.get("insurancepolicyholdersex") //Need to analyse values of it
                    if (insurance.get("policynumber"))
                        insuranceSeg.policyNumber = insurance.get("policynumber")
                    if (insurance.get("insuranceid"))
                        insuranceSeg.healthPlanID = insurance.get("insuranceid").toString()
                    if (insurance.get("insurancetype"))
                        insuranceSeg.planType = insurance.get("insurancetype")
                    if (insurance.get("eligibilitystatus"))
                        insuranceSeg.reportOfEligibilityFlag = insurance.get("eligibilitystatus").equalsIgnoreCase("eligible") ? true : false //This is a boolean, need master for it

                    insuranceSeg.insuranceCompanyID = insurance.get("insurancepackageid") ? insurance.get("insurancepackageid").toString() : null
                    if (insurance.get("ircname"))
                        insuranceSeg.insuranceCompanyName = insurance.get("ircname").toString()
                    if (insurance.get("insurancephone"))
                        insuranceSeg.insuranceCompanyPhoneNumbers = athenaUtil.createTelecomDTO([["value":insurance.get("insurancephone"), "system":"phone", "useCode":"home"]])
                    Map companyAddress = new LinkedHashMap()
                    companyAddress.put("line1", insurance.get("insurancepackageaddress1"))
                    companyAddress.put("line2", insurance.get("insurancepackageaddress2"))
                    companyAddress.put("city", insurance.get("insurancepackagecity"))
                    companyAddress.put("state", insurance.get("insurancepackagestate"))
                    companyAddress.put("zip", insurance.get("insurancepackagezip"))
                    insuranceSeg.insuranceCompanyAddresses = athenaUtil.createAddressDTOList([companyAddress])

                    hl7Message.inSegments.add(insuranceSeg)
                }
            } else
                log.info("createHl7DTOFieldsFromInsuranceAndGuarantor() : No insurance data found in patient response!")
        } else
            log.info("createHl7DTOFieldsFromInsuranceAndGuarantor() : No patient information available")

        return hl7Message
    }

    public def createReferenceHL7ForPatientLevelData(Map requestMap) {

        try {
            log.info("[createReferenceHL7ForPatientLevelData] Going to fetch patient with status "+athenaConst.CDF)
            Map patientRequestMap = [
                    "apiName": athenaConst.PATIENT_REFERENCE,
                    "referenceApiList" : requestMap?.referenceApiList,
                    "messageType":requestMap.hl7Type,
                    "jobInstanceNumber": requestMap.jobInstanceNumber,
                    "limit": athenaConf.fetchLimitForHl7,
                    "practiceID": requestMap.practiceID,
                    "sourceOfOrigin":requestMap.sourceOfOrigin
            ]
            Map patientSearchMap = createSearchMapForReference(athenaConst.REFERENCE_HL7,patientRequestMap)
            Map patientResponseMap = athenaUtil.fetchAthenaData(patientSearchMap, true, athenaConst.PROCESSING)
            if (patientResponseMap?.get("totalcount") > 0) {
                for (def patientObj : patientResponseMap?.get("objects")) {
                    Map hl7CreateMap = [
                            "patientObj":patientObj,
                            "hl7Object": null,
                            "eventType": requestMap.eventType,
                            "messageType": requestMap.hl7Type
                    ]
                    createReferenceHL7(patientObj,hl7CreateMap,requestMap)
                }
            }else{
                log.info("[createReferenceHL7ForPatientLevelData] No pending data found for ${requestMap?.hl7Type}.")
            }
        }catch (Exception ex){
            log.error("[createReferenceHL7ForPatientLevelData] Some exception occurred while creating HL7Message.",ex)
        }
    }

    public def createReferenceHL7ForEncounterLevelData(Map requestMap) {

                    try {
            log.info("[createReferenceHL7ForEncounterLevelData] Going to fetch encounter/s with status "+athenaConst.CDF)
            Map encounterRequestMap = [
                    "apiName": athenaConst.ENCOUNTER_REFERENCE,
                    "messageType":requestMap.hl7Type,
                    "jobInstanceNumber": requestMap.jobInstanceNumber,
                    "limit": athenaConf.fetchLimitForHl7,
                    "practiceID": requestMap.practiceID,
                    "sourceOfOrigin":requestMap.sourceOfOrigin
            ]
            if(!(requestMap.hl7Type == athenaConst.VXU || requestMap.hl7Type == "RAS")){
                encounterRequestMap.put("referenceApiList" , requestMap?.referenceApiList)
            }
            Map encounterSearchMap = createSearchMapForReference(athenaConst.REFERENCE_HL7,encounterRequestMap)
            Map encounterResponseMap = athenaUtil.fetchAthenaData(encounterSearchMap, true, athenaConst.PROCESSING)
            if (encounterResponseMap?.get("totalcount") > 0) {
                for (def encounterObj : encounterResponseMap?.get("objects")) {
                    def patientObj = mongoService.search(new BasicDBObject("patientId",encounterObj?.patientId).append("apiName",athenaConst.PATIENT_REFERENCE).append("stagingId",encounterObj?.stagingId),athenaConst.REFERENCE_COLLECTION)?.objects?.getAt(0)
                        if (!patientObj) {
                            log.error("Not able to process further as patient data not available");
                        encounterObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(requestMap?.hl7Type)}?.status = athenaConst.FAILED;
                        encounterObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(requestMap?.hl7Type)}?.retries += 1;
                        encounterObj?.lastUpdated = new Date();
                        encounterObj?.errorMessage = "Patient Data Not Found";
                        mongoService.update(new BasicDBObject("_id",encounterObj._id),new BasicDBObject("\$set",encounterObj) ,athenaConst.REFERENCE_COLLECTION);
                        continue;
                    }else if((requestMap?.hl7Type == athenaConst.VXU && !patientObj?.referenceDataFetchedSuccessFully?.contains(athenaConst.IMMUNIZATION_CLINICALREFERENCE)) || (requestMap?.hl7Type == "RAS" && !patientObj?.referenceDataFetchedSuccessFully?.contains(athenaConst.MEDICATIONADMINISTRATION_CLINICALREFERENCE))){
                        encounterObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(requestMap?.hl7Type)}?.status = athenaConst.PENDING;
                        mongoService.update(new BasicDBObject("_id",encounterObj._id),new BasicDBObject("\$set",encounterObj) ,athenaConst.REFERENCE_COLLECTION);
                        continue;
                    }

                    Map hl7CreateMap = [
                            "encounterObj":encounterObj,
                            "patientObj":patientObj,
                            "hl7Object": null,
                            "eventType": requestMap.eventType,
                            "messageType": requestMap.hl7Type
                    ]
                    createReferenceHL7(encounterObj,hl7CreateMap,requestMap)
                }
                        } else {
                log.info("[createReferenceHL7ForEncounterLevelData] No pending data found for ${requestMap?.hl7Type}.")
                                            }
        }catch (Exception ex){
            log.error("[createReferenceHL7ForEncounterLevelData] Some exception occurred while creating HL7Message.",ex)
                                        }
                                    }

    public def createReferenceHL7(def referenceDataObj,Map hl7CreateMap,Map requestMap){
        boolean isHl7MessageSaved = false
                                    List clinicalDataIds = []
        boolean isClinicalData = false
        int retries = 0;
        hl7CreateMap.put("appointmentObj", mongoService.search(new BasicDBObject("_id", hl7CreateMap?.patientObj?.stagingId).append("apiName", athenaConst?.APPOINTMENT_SUBSCRIPTION), athenaConst.STAGING_COLLECTION)?.objects?.getAt(0))
        if(requestMap.hl7Type == "SIU"){
            isClinicalData = true
            if (hl7CreateMap?.appointmentObj?.appointmentSubscription?.encounterid) {
                hl7CreateMap.put("encounterObj", mongoService.search(new BasicDBObject("encounterId", hl7CreateMap?.appointmentObj?.appointmentSubscription?.encounterid).append("apiName", "encounterReference"), athenaConst.REFERENCE_COLLECTION)?.objects?.getAt(0))
            }
            hl7CreateMap = createClinicalHl7DTO(hl7CreateMap);
        }else {
                                    for (String referenceApi : requestMap.referenceApiList) {
            log.info("[createReferenceHL7ForPatientLevelData] Going to fetch clinicalData for apiName "+referenceApi);
            Map clinicalDataRequestMap = [
                    "apiName": referenceApi,
                    "patientId": referenceDataObj?.patientId,
                    "stagingId":referenceDataObj?.stagingId,
                    "messageType":requestMap.hl7Type,
                    "jobInstanceNumber": requestMap.jobInstanceNumber
            ]

            if(referenceDataObj?.apiName?.equals("encounterReference")){
                clinicalDataRequestMap.put("encounterId",referenceDataObj?.encounterId)
                                            }

            Map clinicalDataSearchMap = createSearchMapForReference(athenaConst.CLINICAL_HL7, clinicalDataRequestMap);
                                            while(true) {
                                                Map clinicalDataResponseMap = athenaUtil.fetchAthenaData(clinicalDataSearchMap, true, "PROCESSING_HL7")
                                                if (clinicalDataResponseMap.get("totalcount") > 0) {
                                                    isClinicalData = true
                    for(def clinicalDataObj : clinicalDataResponseMap.get("objects")){
                                                        Map validationMap = athenaUtil.validateClinicalData(clinicalDataObj)
                                                        if (!validationMap.isValid){
                                                            BasicDBObject allEntryStatusUpdate = new BasicDBObject("status", "FAILED_HL7").append("lastUpdated", new Date()).append("retries", clinicalDataObj.retries + 1).append("errorMessage", validationMap.errorMessage)
                                                            athenaUtil.updateAthenaCollection(allEntryStatusUpdate, clinicalDataObj._id, "athenaReferenceData")
                                                            continue
                                                        }
                        retries = clinicalDataObj?.retries
                                                        clinicalDataIds.add(clinicalDataObj._id);
                                                        hl7CreateMap.put("clinicalDataObj", clinicalDataObj)
                                                        hl7CreateMap.put("isProcessed", false)
                        if(requestMap.hl7Type?.equals("MDMCL")){
                            hl7CreateMap.put("encounterObj" ,mongoService.search(new BasicDBObject("encounterId",clinicalDataObj?.encounterId).append("apiName","encounterReference"),athenaConst.REFERENCE_COLLECTION)?.objects?.getAt(0))
                        }
                        hl7CreateMap = createClinicalHl7DTO(hl7CreateMap);
                        if (athenaConf?.oneRecordOneHl7List?.contains(requestMap?.hl7Type)){
                                                            try {
                                                                if (hl7CreateMap.exceptionMessage && hl7CreateMap.exceptionMessage != null) {
                                    throw hl7CreateMap.exceptionMessage
                                                                }
                                                                if (hl7CreateMap.hl7Object.save(flush: true)) {
                                                                    isHl7MessageSaved = true
                                                                } else {
                                                                    isHl7MessageSaved = false
                                                                    hl7CreateMap.isProcessed = false
                                                                }
                                                                hl7CreateMap.put("hl7Object", null)
                                                            } catch (Exception ex) {
                                                                log.error("Exception occured while creating HL7 message", ex)
                                                                isHl7MessageSaved = false

                                                                BasicDBObject allEntriesStatusUpdate = new BasicDBObject("status", "FAILED_HL7").append("lastUpdated", new Date()).append("retries", clinicalDataObj.retries + 1).append("errorMessage", ex.getMessage())
                                                                athenaUtil.updateAthenaCollection(allEntriesStatusUpdate, clinicalDataObj._id, "athenaReferenceData")
                                                                hl7CreateMap.put("hl7Object", null)
                                                            }
                                                        }
                                                        BasicDBObject clinicalStatusDataUpdate
                                                        if (hl7CreateMap.isProcessed) {
                            clinicalStatusDataUpdate = new BasicDBObject("status", "PROCESSED_HL7").append("lastUpdated", new Date()).append("processedDate", new Date())
                                                        } else {
                            log.error("Exception occured while creating HL7 message", hl7CreateMap?.exceptionMessage)
                            clinicalStatusDataUpdate = new BasicDBObject("status", "FAILED_HL7").append("lastUpdated", new Date()).append("retries", clinicalDataObj.retries + 1).append("errorMessage", hl7CreateMap?.exceptionMessage?.getMessage())
                                                        }
                                                        athenaUtil.updateAthenaCollection(clinicalStatusDataUpdate, clinicalDataObj._id, "athenaReferenceData")

                                                    }
                    }else if((requestMap?.hl7Type?.equals("ADT") || requestMap?.hl7Type?.equals("ADT_ALG")) && hl7CreateMap.hl7Object == null){
                                                    isClinicalData = true
                                                    hl7CreateMap.put("isProcessed", false)
                                                    hl7CreateMap = createClinicalHl7DTO(hl7CreateMap);
                                                }

                                                if(clinicalDataResponseMap.get("totalcount") < 100){
                                                    break;
                                                }
                                            }
                                    }
        }
                                    if (hl7CreateMap.hl7Object) {
                                        try {
                if (!athenaConf?.oneRecordOneHl7List?.contains(requestMap?.hl7Type)) {
                                                if(hl7CreateMap.exceptionMessage && hl7CreateMap.exceptionMessage != null){
                        throw hl7CreateMap.exceptionMessage
                                                }
                                                if(hl7CreateMap.hl7Object.save(flush: true)) {
                                                    isHl7MessageSaved = true
                                                } else {
                                                    isHl7MessageSaved = false
                                                    BasicDBObject allEntriesStatusUpdate = new BasicDBObject("status", "FAILED_HL7").append("lastUpdated", new Date()).append("retries", retries+1).append("errorMessage", "Unable to Save HL7 Message.")
                                                    athenaUtil.updateAthenaCollection(allEntriesStatusUpdate, clinicalDataIds, "athenaReferenceData")
                                                }
                                            }
                                        } catch (Exception ex) {
                                            log.error("Exception occured while creating HL7 message",ex)
                                            isHl7MessageSaved = false
                                            BasicDBObject allEntriesStatusUpdate = new BasicDBObject("status", "FAILED_HL7").append("lastUpdated", new Date()).append("retries", retries+1).append("errorMessage", ex.getMessage())
                                            athenaUtil.updateAthenaCollection(allEntriesStatusUpdate, clinicalDataIds, "athenaReferenceData")
                                        }
                                    }

        updateReferenceDataStatus(referenceDataObj, requestMap.hl7Type, isHl7MessageSaved, isClinicalData)
                                    }

    private void updateReferenceDataStatus(Map referenceDataObj,String hl7Type,Boolean isHl7MessageSaved,Boolean isClinicalData){
        log.info("(updateAppointmentReferenceStatus) -> Going to update status of referenceData ${referenceDataObj?.apiName} | id: ${referenceDataObj._id}| hl7CreationStatusMap for type ${hl7Type}");

        if (isHl7MessageSaved) {
            referenceDataObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(hl7Type)}?.status = athenaConst.PROCESSED;
            if (referenceDataObj.hl7ProcessedDate == null) {
                referenceDataObj.hl7ProcessedDate = [:]
                }
            referenceDataObj.hl7ProcessedDate[hl7Type] = new Date();
            }else {
            if(!isClinicalData){
                referenceDataObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(hl7Type)}?.status = athenaConst.NO_DATA_FOUND;
        }else {
                referenceDataObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(hl7Type)}?.status = athenaConst.FAILED;
                referenceDataObj?.hl7CreationStatusMap?.find{it?.messageType?.equals(hl7Type)}?.retries += 1;
                }
            }
        referenceDataObj?.lastUpdated = new Date();
        mongoService.update(new BasicDBObject("_id",referenceDataObj._id),new BasicDBObject("\$set",referenceDataObj) ,athenaConst.REFERENCE_COLLECTION)
    }

    public Map createClinicalHl7DTO(Map requestMap) {
        BasicDBObject appointmentObject = requestMap.get("appointmentObj")
        BasicDBObject encounterObject = requestMap.get("encounterObj")
        BasicDBObject clinicalDataObject = requestMap.get("clinicalDataObj")
        BasicDBObject patientObject = requestMap.get("patientObj")
        if (requestMap.get("hl7Object") == null) {
            try {
                String sourceOfOrigin = fetchSourceOfOrigin(patientObject.get("stagingId"));
                log.info("createHl7DTO Loop : Going to create the Hl7 Meesage for messageType =>" + requestMap.get("messageType"));
                Hl7Message hl7Message = new Hl7Message();
                hl7Message = createHl7DTOFieldsFromPatient(patientObject, hl7Message);
                //@TODO logic to remove hyphens from bod to put that in mod field && also need to fetch patient obj
                hl7Message.modField = patientObject?.resource?.get("birthDate")?.replaceAll("-","")
                hl7Message.messageId = clinicalDataObject?.get("_id") ?: patientObject?._id
                hl7Message.messageType = (clinicalDataObject?.apiName == "clinicalImpressionClinicalReference")? "MDM" : (requestMap.get("messageType").equals("ADT_ALG"))? "ADT" : (requestMap.get("messageType").equals("VXU_NE")) ? "VXU" : requestMap.get("messageType");
                hl7Message.eventType = requestMap.get("eventType");
                hl7Message.processed = athenaConst.PENDING;
                hl7Message.practitionerList = new LinkedList<>();
                hl7Message.metadata = [
                        "DATA_SOURCE_ID": athenaConf?.metaDataMap?.get(patientObject?.practiceId)?.dataSourceId,
                        "PARTNER_ID": athenaConf?.metaDataMap?.get(patientObject?.practiceId)?.partnerId,
                        "PRIMARY_IDENTIFIER" : hl7Message.patientId
                ];

                if (requestMap.get("messageType").equals("ADT")) {
                    if(encounterObject == null)
                        hl7Message.patientType = "N";
                    else
                        hl7Message.patientType = encounterObject?.get("resource")?.get("class")?.get("code")
                }
                hl7Message.receivedDate = patientObject.get("dateCreated");
                hl7Message.jobInstanceNumber = athenaUtil.getJobInstanceNumber(hl7Message.modField, hl7Message.messageType);

                //DepartmentReference is mandatory for all data set for whom we are creating the HL7 messages
                BasicDBObject locationSearchMap = new BasicDBObject()
                StringJoiner performingLocationId = new StringJoiner("_").add(patientObject.get("practiceId").toString());
                hl7Message.dateTimeZone=athenaConf.athenaPracticeIds?.get(patientObject.get("practiceId"))?.get("timeZone")
                if (appointmentObject?.get("apiName")?.equals("appointmentSubscription") && appointmentObject?.get("departmentId")){
                    performingLocationId.add(appointmentObject?.get("departmentId")?.toString());
                    locationSearchMap.append("masterId",appointmentObject?.get("departmentId")?.toString())
                    locationSearchMap.append("practiceId",appointmentObject?.get("practiceId")?.toString())
                    hl7Message.dateTimeZone=athenaConf.athenaPracticeIds?.get(appointmentObject.get("practiceId"))?.get("timeZone")
                } else if (encounterObject != null){
                    performingLocationId.add(encounterObject?.get("departmentId")?.toString());
                    locationSearchMap.append("masterId",encounterObject?.get("departmentId")?.toString())
                    locationSearchMap.append("practiceId",encounterObject?.get("practiceId")?.toString())
                    hl7Message.dateTimeZone=athenaConf.athenaPracticeIds?.get(encounterObject.get("practiceId"))?.get("timeZone")
                }
                hl7Message.performingLocationId = performingLocationId;
                def performingLocationName = mongoService.search(locationSearchMap, "athenaMaster")?.objects?.getAt(0)?.masterJson?.name
                hl7Message.performingLocationName = performingLocationName;
                hl7Message.performingOrganizationId = athenaConf?.metaDataMap?.get(patientObject?.practiceId)?.partnerId;
                hl7Message.performingOrganizationName = athenaConf?.metaDataMap?.get(patientObject?.practiceId)?.partnerName;
                hl7Message.sendingFacility = athenaConf.sendingFacility


                if (requestMap.get("messageType").equals("VXU") || requestMap.get("messageType").equals("VXU_NE")){
                    hl7Message.immunizationSegments = createHl7DTOFieldsFromImmunization(clinicalDataObject as Map, hl7Message.immunizationSegments)
                }

                if (requestMap.get("messageType").equals("RDE")){
                    hl7Message.rdeSegments = createHl7DTOFieldsFromRDESegment(clinicalDataObject as Map, hl7Message.rdeSegments)
                }

                if (requestMap.get("messageType").equals("RAS")){
                    hl7Message.rasSegments = createHl7DTOFieldsFromRASSegment(clinicalDataObject as Map, hl7Message.rasSegments)
                }

                if (requestMap.get("messageType").equals("GOL")){
                    hl7Message = createHl7DTOForGoal(clinicalDataObject as Map, hl7Message)
                }
                if (requestMap.get("messageType").equals("ORU")){
                    hl7Message = createHl7DTOForORU(clinicalDataObject as Map, hl7Message)
                }

                if(hl7Message.messageType == "MDM" ){
                    hl7Message  = createHl7DTOFieldsFromDocumentReference( hl7Message, clinicalDataObject);
                }

                if(requestMap.messageType == "ADT" && clinicalDataObject){
                    if(appointmentObject?.appointmentSubscription?.providerid){
                        hl7Message.practitionerList.addAll(createHl7DTOFieldsFromPractitioner(appointmentObject, StringConstants.PRIMARY_PERFORMER_DOCTOR, ["reference": "Practitioner/"+appointmentObject?.appointmentSubscription?.providerid]))
                    }
                    if(clinicalDataObject?.apiName == "conditionDiagnosisClinicalReference"){
                        hl7Message = createHl7MessageFieldsFromCondition(clinicalDataObject,hl7Message);
                    }
                    if(clinicalDataObject?.apiName == "procedureClinicalReference"){
                        hl7Message = createHl7DTOFieldsFromProcedure(clinicalDataObject, hl7Message);
                    }
                    if(clinicalDataObject?.apiName == "observationClinicalReference"){
                        hl7Message = createHl7DTOFieldsFromObservation(clinicalDataObject, hl7Message);
                    }
                }

                if(requestMap.messageType == "ADT_ALG" && clinicalDataObject){
                    if(clinicalDataObject?.apiName == "allergyIntoleranceClinicalReference"){
                        hl7Message = createHl7MessageFieldsFromAllergyIntolerance(clinicalDataObject,hl7Message);
                    }
                }

                if(requestMap.messageType == "PPR" && clinicalDataObject){
                    hl7Message = createHl7MessageFieldsFromCondition(clinicalDataObject, hl7Message);
                }

                if(requestMap.messageType == "SIU"){
                    hl7Message = createHl7DTOFieldsFromAppointment(appointmentObject,hl7Message)
                }

                if(clinicalDataObject?.apiName == "allergyIntoleranceClinicalReference" || clinicalDataObject?.apiName == "goalClinicalReference"|| clinicalDataObject?.apiName == "conditionProblemClinicalReference" || requestMap.get("messageType").equals("VXU_NE")){
                    hl7Message.patientType = "N";
                }
                else{
                    hl7Message = createHl7DTOFieldsFromEncounter(encounterObject, hl7Message,clinicalDataObject);
                }

                if(encounterObject== null && clinicalDataObject == null && (requestMap.messageType == "ADT_ALG" || requestMap.messageType == "ADT")){
                    hl7Message.patientType = "N";
                }

                requestMap.hl7Object = hl7Message
                requestMap.isProcessed = true
            } catch (Exception ex){
                requestMap.put("exceptionMessage", ex)
                requestMap.isProcessed = false
            }

        } else {
            try {
                Hl7Message hl7Message = requestMap.hl7Object
                if (requestMap.get("messageType").equals("VXU") || requestMap.get("messageType").equals("VXU_NE")){
                    hl7Message.immunizationSegments = createHl7DTOFieldsFromImmunization(clinicalDataObject as Map, hl7Message.immunizationSegments)
                }

                if (requestMap.get("messageType").equals("RDE")){
                    hl7Message.rdeSegments = createHl7DTOFieldsFromRDESegment(clinicalDataObject as Map, hl7Message.rdeSegments)
                }

                if (requestMap.get("messageType").equals("RAS")){
                    hl7Message.rasSegments = createHl7DTOFieldsFromRASSegment(clinicalDataObject as Map, hl7Message.rasSegments)
                }

                if (requestMap.get("messageType").equals("GOL")){
                    hl7Message = createHl7DTOForGoal(clinicalDataObject as Map, hl7Message)
                }

                if (requestMap.get("messageType").equals("ORU")){
                    hl7Message = createHl7DTOForORU(clinicalDataObject as Map, hl7Message)
                }

                if(requestMap.get("messageType").equals("ADT") && clinicalDataObject){
                    if(appointmentObject?.appointmentSubscription?.providerid){
                        hl7Message.practitionerList.addAll(createHl7DTOFieldsFromPractitioner(appointmentObject, StringConstants.PRIMARY_PERFORMER_DOCTOR, ["reference": "Practitioner/"+appointmentObject?.appointmentSubscription?.providerid]))
                    }
                    if(clinicalDataObject.apiName == "conditionDiagnosisClinicalReference"){
                        hl7Message = createHl7MessageFieldsFromCondition(clinicalDataObject,hl7Message);
                    }
                    if(clinicalDataObject.apiName == "procedureClinicalReference"){
                        hl7Message = createHl7DTOFieldsFromProcedure(clinicalDataObject, hl7Message);
                    }
                    if(clinicalDataObject.apiName == "observationClinicalReference"){
                        hl7Message = createHl7DTOFieldsFromObservation(clinicalDataObject, hl7Message);
                    }
                }

                if(requestMap.messageType == "ADT_ALG" && clinicalDataObject){
                    if(clinicalDataObject?.apiName == "allergyIntoleranceClinicalReference"){
                        hl7Message = createHl7MessageFieldsFromAllergyIntolerance(clinicalDataObject,hl7Message);
                    }
                }

                if(requestMap.messageType == "PPR" && clinicalDataObject){
                    hl7Message = createHl7MessageFieldsFromCondition(clinicalDataObject, hl7Message);
                }

                if (requestMap.get("messageType").equals("SIU")){
                    hl7Message = createHl7DTOFieldsFromAppointment(requestMap,hl7Message)
                }

                if(hl7Message.messageType == "MDM" ){
                    hl7Message  = createHl7DTOFieldsFromDocumentReference( hl7Message, clinicalDataObject);
                }

                requestMap.hl7Object = hl7Message
                requestMap.isProcessed = true
            } catch (Exception ex){
                requestMap.put("exceptionMessage", ex)
                requestMap.isProcessed = false
            }
        }
        return requestMap;
    }

    private String fetchSourceOfOrigin(def stagingId){
        BasicDBObject searchQuery = new BasicDBObject("_id",stagingId);
        def stagingObject = mongoService.search(searchQuery, athenaConst.STAGING_COLLECTION)?.objects?.getAt(0)
        String apiNameValue = stagingObject?.get("apiName")?.toString()
        return apiNameValue?.equals("appointmentSubscription")? "CDC" : "HistoricalData";
    }

    private Hl7Message createHl7MessageFieldsFromAllergyIntolerance(def allergyObject,Hl7Message hl7Message){

        AL1Segment al1Segment = new AL1Segment()
        IdentifierDTO identifierDTO = new IdentifierDTO();
        if(allergyObject?.resource?.identifier){
            identifierDTO.identifier = allergyObject?.resource?.identifier?.getAt(0)?.value?.split("-")?.last()
        }else{
            identifierDTO.identifier = allergyObject?.resource?.id?.split("-")?.last();
        }
        List<IdentifierDTO> identifier = [] as List<IdentifierDTO>;
        identifier.add(identifierDTO);
        al1Segment.identifier = identifier;
        al1Segment.onset = allergyObject?.resource?.onset?.toString();
        al1Segment.lastOccurrence = allergyObject?.resource?.recordedDate
        List<ReactionDTO> reaction = [] as List<ReactionDTO>;
        for(int i=0;i<allergyObject?.resource?.reaction?.size();i++) {
            ReactionDTO reactionDTO = new ReactionDTO();
            reactionDTO.description = allergyObject?.resource?.reaction?.getAt(i)?.description;
            reactionDTO.manifestations = athenaUtil.getCodeableConceptDTO(allergyObject?.resource?.reaction?.getAt(i)?.manifestation);
            reactionDTO.notesText = allergyObject?.resource?.reaction?.getAt(i)?.note?.getAt(0)?.text
            reactionDTO.severity = allergyObject?.resource?.reaction?.getAt(i)?.severity
            reaction.add(reactionDTO);
        }
        al1Segment.reaction = reaction;
        al1Segment.clinicalStatus = athenaUtil.getCodeableConceptDTO( allergyObject?.resource?.clinicalStatus);
        al1Segment.verificationStatus = athenaUtil.getCodeableConceptDTO( allergyObject?.resource?.verificationStatus);
        //String type
        al1Segment.category =  allergyObject?.resource?.category;
        al1Segment.criticality = allergyObject?.resource?.criticality?.getAt(0);
        al1Segment.code = athenaUtil.getCodeableConceptDTO( allergyObject?.resource?.code);
        al1Segment.notes = athenaUtil.createAnnotationDTOList(allergyObject?.resource?.note);
        if(hl7Message.al1Segments == null){
            List<AL1Segment> al1Segments = [] as List<AL1Segment>;
            al1Segments.add(al1Segment);
            hl7Message.al1Segments = al1Segments;
        }else{
            hl7Message.al1Segments.add(al1Segment)
        }
        return hl7Message;
    }

    private Hl7Message createHl7MessageFieldsFromCondition(def conditionObject,Hl7Message hl7Message) {

        ConditionSegment conditionSegment = new ConditionSegment();
        IdentifierDTO identifierDTO = new IdentifierDTO();
        if (conditionObject?.resource?.identifier) {
            identifierDTO.identifier = conditionObject?.resource?.identifier?.getAt(0)?.value?.split("-")?.last()
        } else {
            identifierDTO.identifier = conditionObject?.resource?.id?.split("-")?.last();
        }
        List<IdentifierDTO> identifier = [] as List<IdentifierDTO>;
        identifier.add(identifierDTO);
        conditionSegment.identifier = identifier;
        conditionSegment.onset = conditionObject?.resource?.onsetDateTime?.toString();
        conditionSegment.abatement = conditionObject?.resource?.abatementDateTime?.toString();
        conditionSegment.assertedDate = conditionObject?.resource?.recordedDate
        conditionSegment.clinicalStatus = athenaUtil.getCodeableConceptDTO( conditionObject?.resource?.clinicalStatus);
        conditionSegment.verificationStatus = athenaUtil.getCodeableConceptDTO( conditionObject?.resource?.verificationStatus);
        conditionSegment.severity = athenaUtil.getCodeableConceptDTO( conditionObject?.resource?.severity);
        conditionSegment.category = athenaUtil.getCodeableConceptDTO( conditionObject?.resource?.category);
        conditionSegment.bodySite = athenaUtil.getCodeableConceptDTO( conditionObject?.resource?.bodySite) ;
        conditionSegment.code = athenaUtil.getCodeableConceptDTO(conditionObject?.resource?.code)
        conditionSegment.practitionerAsserter =  createHl7DTOFieldsFromPractitioner(conditionObject,null,conditionObject?.resource?.recorder)?.getAt(0);
        List<AnnotationDTO> notes = [] as List<AnnotationDTO>
        for(def note : conditionObject?.resource?.note){
        AnnotationDTO annotationDTO = new AnnotationDTO();
        //annotationDTO.practitionerAuthor @TODO practitioner of reference type
        //PractitionerDTO practitionerAuthor
            annotationDTO.text = note?.text
            annotationDTO.time = note?.time
            notes.add(annotationDTO);
        }
        conditionSegment.note = notes;
        if (hl7Message.conditionSegments == null){
            List<ConditionSegment> conditionSegments = [] as List<ConditionSegment>;
            conditionSegments.add(conditionSegment);
            hl7Message.conditionSegments = conditionSegments;
        }else{
            hl7Message.conditionSegments.add(conditionSegment)
        }
        return hl7Message;
    }

    private Hl7Message createHl7DTOFieldsFromProcedure(def procedureObject,Hl7Message hl7Message){

        PR1Segment pr1Segment = new PR1Segment();
        pr1Segment.identifier = athenaUtil.valueFormatter(procedureObject?.resource?.identifier?.getAt(0)?.value,"\\.",".",2);
        pr1Segment.code = procedureObject?.resource?.code?.coding?.getAt(0)?.code
        pr1Segment.name = procedureObject?.resource?.code?.text
        pr1Segment.codeSystem = procedureObject?.resource?.code?.coding?.getAt(0)?.system
        pr1Segment.performedDate = procedureObject?.resource?.performedDateTime
        pr1Segment.type = procedureObject?.resource?.resourceType
        pr1Segment.status = procedureObject?.resource?.status
        pr1Segment.performer  = createHl7DTOFieldsFromPractitioner(procedureObject,null, procedureObject?.resource?.performer?.find{it?.actor?.reference?.contains("Practitioner/")}?.actor)?.getAt(0);
        PeriodDTO performedPeriod = new PeriodDTO();
        performedPeriod.start = procedureObject?.resource?.performedPeriod?.start
        performedPeriod.end = procedureObject?.resource?.performedPeriod?.end
        pr1Segment.performedPeriod = performedPeriod;
        pr1Segment.procedureCode = athenaUtil.getCodeableConceptDTO( procedureObject?.resource?.code)//CodeableConceptDTO
        AnnotationDTO annotationDTO = new AnnotationDTO();
        annotationDTO.text = procedureObject?.resource?.note?.text
        List<AnnotationDTO> notes = [] as List<AnnotationDTO>;
        notes.add(annotationDTO);
        pr1Segment.notes = notes;
        if(hl7Message.pr1Segments == null){
            List<PR1Segment> pr1Segments = [] as List<PR1Segment>;
            pr1Segments.add(pr1Segment);
            hl7Message.pr1Segments = pr1Segments;
        }else{
            hl7Message.pr1Segments.add(pr1Segment);
        }
        return hl7Message;
    }

    private Hl7Message createHl7DTOFieldsFromObservation(def observationObject,Hl7Message hl7Message){

        ObservationSegment observationSegment = new ObservationSegment();
        //String applicationId
        observationSegment.paramId = observationObject?.resource?.code?.coding?.getAt(0)?.code
        observationSegment.paramName = observationObject?.resource?.code?.coding?.getAt(0)?.display

        if(observationObject?.resource?.valueQuantity?.value){
        observationSegment.resultValue = observationObject?.resource?.valueQuantity?.value
        }else if(observationObject?.resource?.valueString){
            observationSegment.resultValue = observationObject?.resource?.valueString
        }
        observationObject.valueString = observationObject?.resource?.valueString
        observationSegment.unitName = observationObject?.resource?.valueQuantity?.unit
        observationSegment.referenceRange = observationObject?.resource?.referenceRange?.getAt(0)?.text
        observationSegment.codingSystem = observationObject?.resource?.code?.coding?.getAt(0)?.system
        observationSegment.observationType = "NM"//observationObject?.resource?.code?.coding?.getAt(0)?.display;
        observationSegment.category = observationObject?.resource?.category?.getAt(0)?.coding?.getAt(0)?.code ?: "vital-signs"
        observationSegment.effective = observationObject?.resource?.effectiveDateTime;
        observationSegment.resultDateStr = observationObject?.resource?.effectiveDateTime ?: observationObject?.resource?.issued;
        List<IdentifierMetaData> identifiers = [];
        IdentifierMetaData identifierMetaData = new IdentifierMetaData();
        identifierMetaData.assigningAuthority =observationObject?.resource?.identifier?.getAt(0)?.system;
        identifierMetaData.idValue = athenaUtil.valueFormatter(observationObject?.resource?.identifier?.getAt(0)?.value,"-","-",2);
        identifiers.add(identifierMetaData);
        observationSegment.identifiers = identifiers;
        observationSegment.readingUniqueId = identifiers?.getAt(0)?.idValue + hl7Message.patientId
        observationSegment.codeText = observationObject?.resource?.code?.text ? observationObject?.resource?.code?.text : observationObject?.resource?.code?.coding?.getAt(0)?.display
        observationSegment.code = observationObject?.resource?.code?.coding?.getAt(0)?.code
        String observationStatusStr = observationObject?.resource?.status
        if(athenaConf?.observationStatus?.containsKey(observationObject?.resource?.status)){
            observationStatusStr = athenaConf?.observationStatus?.get(observationObject?.resource?.status)
        }
        observationSegment.status = observationStatusStr
        observationSegment.testResultStatus = observationStatusStr
        if(observationObject?.resource?.valueQuantity){
        observationSegment.valueCodeableConceptDTO = athenaUtil.getCodeableConceptDTO(observationObject?.resource?.valueQuantity?.code.toString(),observationObject?.resource?.valueQuantity?.value.toString(),observationObject?.resource?.valueQuantity?.system.toString(),observationObject?.resource?.valueQuantity?.unit.toString())
        }else if(observationObject?.resource?.valueCodeableConcept){
            observationSegment.valueCodeableConceptDTO = athenaUtil.getCodeableConceptDTO(observationObject?.resource?.valueCodeableConcept)
        }
        observationSegment.interpretationCodes = athenaUtil.getCodeableConceptDTO(observationObject?.resource?.interpretation)
        observationSegment.performers = createHl7DTOFieldsFromPractitioner(observationObject,null, observationObject?.resource?.performer)
        observationSegment.components = createComponentsDTO(observationObject?.resource?.component)
        if(hl7Message.obxSegments == null){
            List<ObservationSegment> obxSegments = [];
            obxSegments.add(observationSegment);
            hl7Message.obxSegments = obxSegments
        }else{
            hl7Message.obxSegments.add(observationSegment);
        }
        return hl7Message
    }

    private List<ObxSegment> createComponentsDTO(List<Map> componentsList){
        if(componentsList) {
            List<ObxSegment> components = new LinkedList<>();
            for (Map componentData : componentsList) {
                ObxSegment component = new ObxSegment();
                component.resultValue = componentData?.valueQuantity?.value
                component.unitName = componentData?.valueQuantity?.unit
                component.valueCodeableConceptDTO = athenaUtil.getCodeableConceptDTO(componentData?.valueQuantity?.code.toString(), componentData?.valueQuantity?.value.toString(), componentData?.valueQuantity?.system.toString(), componentData?.valueQuantity?.unit.toString())
                component.paramId = componentData?.code?.coding?.getAt(0)?.code
                component.paramName = componentData?.code?.coding?.getAt(0)?.display
                component.codingSystem = componentData?.code?.coding?.getAt(0)?.system
                component.codeText = componentData?.code?.text ? componentData?.code?.text : componentData?.code?.coding?.getAt(0)?.display
                component.code = componentData?.code?.coding?.getAt(0)?.code
                component.referenceRange = componentData?.referenceRange?.getAt(0)?.text
                component.interpretationCodes = athenaUtil.getCodeableConceptDTO(componentData?.interpretation)
                components.add(component)
            }
            return components
        }else{
            return null
        }
    }

    private List<ImmunizationDTO> createHl7DTOFieldsFromImmunization(Map requestMap, List<ImmunizationDTO> immunizationSegments) {
        ImmunizationDTO immunizationDTO = new ImmunizationDTO()

        for(Map performerMap: requestMap.get("resource")?.get("performer")){
            if (performerMap && performerMap?.actor?.reference?.contains("Practitioner/a")){
                immunizationDTO.performer = createHl7DTOFieldsFromPractitioner(requestMap,performerMap?.function?.coding?.getAt(0)?.code,performerMap?.actor);
            }
        }

        if (requestMap.get("resource")?.get("lotNumber")){
            immunizationDTO.lotNumber = requestMap.get("resource")?.get("lotNumber")?.toString()
        }

        if (requestMap.get("resource")?.get("status")){
            immunizationDTO.status = requestMap.get("resource")?.get("status")?.toString()
        }

        if (requestMap.get("resource")?.get("identifier")){
            immunizationDTO.identifier = athenaUtil.createIdentifierDTOs(requestMap.get("resource")?.get("identifier"), "immunizationId", "usual")
        }

        if (requestMap.get("resource")?.get("occurrenceDateTime")){
            immunizationDTO.occurrenceDateTime = requestMap.get("resource")?.get("occurrenceDateTime")
        }

        if (requestMap.get("resource")?.get("vaccineCode")) {
            immunizationDTO.vaccineCode = athenaUtil.createCodeableConceptDTO(requestMap.get("resource")?.get("vaccineCode"))
        }

        if (requestMap.get("resource")?.get("isSubpotent")){
            immunizationDTO.isSubpotent = requestMap.get("resource")?.get("isSubpotent")
        }

        if (requestMap.get("resource")?.get("route")){
            immunizationDTO.route = athenaUtil.createCodeableConceptDTO(requestMap.get("resource")?.get("route"))
        }

        if (requestMap.get("resource")?.get("note")){
            immunizationDTO.note = athenaUtil.createAnnotationDTOList(requestMap.get("resource")?.get("note"))
        }

        if (requestMap.get("resource")?.get("doseQuantity")){
            immunizationDTO.doseQuantity = athenaUtil.createQuantity(requestMap.get("resource")?.get("doseQuantity"))
        }

        if (requestMap.get("resource")?.get("site")){
            immunizationDTO.site = athenaUtil.createCodeableConceptDTO(requestMap.get("resource")?.get("site"))
        }

        if (requestMap.get("resource")?.get("manufacturer")){
            immunizationDTO.manufacturer = athenaUtil.createIdentifierDTOs("immunizationId", requestMap.get("resource")?.get("manufacturer")?.get("display"), null)
        }

        if (requestMap.get("resource")?.get("primarySource")){
            immunizationDTO.primarySource = requestMap.get("resource")?.get("primarySource")
        }

        if (requestMap.get("resource")?.get("expirationDate")){
            immunizationDTO.expirationDate = requestMap.get("resource")?.get("expirationDate")
        }

        if (immunizationSegments == null) {
            immunizationSegments = [] as List<ImmunizationDTO>
            immunizationSegments.add(immunizationDTO)
        } else {
            immunizationSegments.add(immunizationDTO)
        }

        return immunizationSegments

    }


    private List<RDESegment> createHl7DTOFieldsFromRDESegment(Map medicationRequestMap, List<RDESegment> rdeSegments) {
        RDESegment rdeSegmentFields = new RDESegment()

        if (medicationRequestMap.get("resource")?.get("note")){
            rdeSegmentFields.medicationRequestNotes = athenaUtil.createAnnotationDTOList(medicationRequestMap.get("resource")?.get("note"))
        }

        if (medicationRequestMap.get("resource")?.get("identifier")){
            rdeSegmentFields.medicationRequestIdentifier = athenaUtil.createIdentifierDTOs(medicationRequestMap.get("resource")?.get("identifier"),"placerIdentifier", "usual")
        }
        else if(medicationRequestMap.get("resource")?.get("id")){
            IdentifierDTO medicationRequestIdentifierDTO = new IdentifierDTO();
            medicationRequestIdentifierDTO.identifier = medicationRequestMap.get("resource")?.get("id")?.split("-")?.last()
            medicationRequestIdentifierDTO.typeCode = "placerIdentifier"
            medicationRequestIdentifierDTO.useCode = "usual"
            List<IdentifierDTO> medicationRequestIdentifierDTOList = new LinkedList<>()
            medicationRequestIdentifierDTOList.add(medicationRequestIdentifierDTO)
            rdeSegmentFields.medicationRequestIdentifier = medicationRequestIdentifierDTOList
        }
        rdeSegmentFields.medicationRequestGroupIdentifier = rdeSegmentFields.medicationRequestIdentifier?.getAt(0)

        if (medicationRequestMap.get("resource")?.get("status")) {
            rdeSegmentFields.medicationRequestStatus = medicationRequestMap.get("resource")?.get("status")
        }

        if (medicationRequestMap.get("resource")?.get("authoredOn")) {
            rdeSegmentFields.medicationRequestAuthoredOn = medicationRequestMap.get("resource")?.get("authoredOn")
        }

        if (medicationRequestMap.get("resource")?.get("medicationCodeableConcept")) {
            rdeSegmentFields.medicationDosageFormIdCode = medicationRequestMap.get("resource")?.get("medicationCodeableConcept")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("route")?.get("coding")?.getAt(0)?.get("display")){
            rdeSegmentFields.requestRouteDesc = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("route")?.get("coding")?.getAt(0)?.get("display")
        }

        if (medicationRequestMap.get("resource")?.get("category")?.getAt(0)){
            rdeSegmentFields.medicationCategoryCodeDto = athenaUtil.getCodeableConceptDTO(medicationRequestMap.get("resource")?.get("category")?.getAt(0))
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseQuantity")?.get("value")) {
            rdeSegmentFields.medicationRequestDoseValue = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseQuantity")?.get("value")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseQuantity")?.get("unit")) {
            rdeSegmentFields.medicationRequestDoseUnit = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseQuantity")?.get("unit")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("text")) {
            rdeSegmentFields.medicationRequestDrugFrequency = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("text")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseAndRate")?.getAt(0)?.get("doseQuantity")?.get("unit")) {
            rdeSegmentFields.medicationRequestDoseUnit = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseAndRate")?.getAt(0)?.get("doseQuantity")?.get("unit")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseAndRate")?.getAt(0)?.get("doseQuantity")?.get("value")) {
            rdeSegmentFields.medicationRequestDoseValue = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("doseAndRate")?.getAt(0)?.get("doseQuantity")?.get("value")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("timing")?.get("repeat")?.get("period")) {
            rdeSegmentFields.medicationRequestDurationValue = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("timing")?.get("repeat")?.get("period")
        }

        if (medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("timing")?.get("repeat")?.get("periodUnit")) {
            rdeSegmentFields.medicationRequestDurationUnit = medicationRequestMap.get("resource")?.get("dosageInstruction")?.getAt(0)?.get("timing")?.get("repeat")?.get("periodUnit")
        }

        if (medicationRequestMap.get("resource")?.get("requester")) {
            rdeSegmentFields.medicationRequestRecorder = createHl7DTOFieldsFromPractitioner(medicationRequestMap, StringConstants.MEDICATION_PRESCRIBER, medicationRequestMap.get("resource")?.get("requester"))
        }

        if (medicationRequestMap.get("resource")?.get("medicationReference")) {
            rdeSegmentFields = createHl7DTOFieldsFromMedication(medicationRequestMap, medicationRequestMap.get("resource")?.get("medicationReference"), rdeSegmentFields)
        }

        if (rdeSegments == null) {
            rdeSegments = []
            rdeSegments.add(rdeSegmentFields)
        } else {
            rdeSegments.add(rdeSegmentFields)
        }
        return rdeSegments

    }

    private List<RASSegment> createHl7DTOFieldsFromRASSegment(Map requestMap, List<RASSegment> rasSegments) {
        RASSegment rasSegmentFields = new RASSegment()

        if (requestMap.get("resource")?.get("note")) {
            rasSegmentFields.administrationNotes = requestMap.get("resource")?.get("note")
        }

        if (requestMap.get("resource")?.get("dosage")?.get("text")) {
            rasSegmentFields.administeredDrugFrequency = requestMap.get("resource")?.get("dosage")?.get("text")
        }

        if (requestMap.get("resource")?.get("dosage")?.get("route")){
            rasSegmentFields.routeDescCodeDto = athenaUtil.createCodeableConceptDTO(requestMap.get("resource")?.get("dosage")?.get("route"))
        }

        if (requestMap.get("resource")?.get("dosage")?.get("route")?.get("coding")?.getAt(0)?.get("display")){
            rasSegmentFields.routeDesc = requestMap.get("resource")?.get("dosage")?.get("route")?.get("coding")?.getAt(0)?.get("display")
        }

        if (requestMap.get("resource")?.get("dosage")?.get("method")) {
            rasSegmentFields.method = requestMap.get("resource")?.get("dosage")?.get("method")
        }

        if (requestMap.get("resource")?.get("dosage")?.get("quantity")?.get("unit")) {
            rasSegmentFields.administeredDosageUnits = requestMap.get("resource")?.get("dosage")?.get("quantity")?.get("unit")
        }

        if (requestMap.get("resource")?.get("dosage")?.get("quantity")?.get("value")) {
            rasSegmentFields.administeredDosageAmount = requestMap.get("resource")?.get("dosage")?.get("quantity")?.get("value")
        }

        rasSegmentFields.medicationPerDose = athenaUtil.createQuantityDTO(requestMap.get("resource")?.get("dosage")?.get("quantity"))

        if (requestMap.get("resource")?.get("dosage")?.get("sitecodeableconcept")) {
            rasSegmentFields.site = requestMap.get("resource")?.get("dosage")?.get("sitecodeableconcept")
        }

        if (requestMap.get("resource")?.get("status")) {
            rasSegmentFields.medicationAdministeredStatus = requestMap.get("resource")?.get("status")
        }

        if (requestMap.get("resource")?.get("identifier")){
            rasSegmentFields.administrationIdentifier = athenaUtil.createIdentifierDTOs(requestMap.get("resource")?.get("identifier"), "placerIdentifier", "usual")
        }
        else if(requestMap.get("resource")?.get("id")){
            IdentifierDTO administrationIdentifierDTO = new IdentifierDTO();
            administrationIdentifierDTO.typeCode = "placerIdentifier"
            administrationIdentifierDTO.useCode = "usual"
            administrationIdentifierDTO.identifier = requestMap.get("resource")?.get("id")?.split("-")?.last()
            List<IdentifierDTO> administrationIdentifierDTOList = new LinkedList<>()
            administrationIdentifierDTOList.add(administrationIdentifierDTO)
            rasSegmentFields.administrationIdentifier = administrationIdentifierDTOList
        }

        if (requestMap.get("resource")?.get("prescription")?.get("reference")){
            IdentifierDTO medicationRequestIdentifierDTO = new IdentifierDTO();
            medicationRequestIdentifierDTO.typeCode = "placerIdentifier"
            medicationRequestIdentifierDTO.useCode = "usual"
            medicationRequestIdentifierDTO.identifier = requestMap.get("resource")?.get("prescription")?.get("reference")?.split("/")[1]?.split("\\?")[0]
            rasSegmentFields.medicationRequestIdentifier = medicationRequestIdentifierDTO
        }

        if (requestMap.get("resource")?.get("reasongiven")) {
            rasSegmentFields.reasonCodes = requestMap.get("resource")?.get("reasongiven")
        }

        if (requestMap.get("resource")?.get("reasonnotgiven")) {
            rasSegmentFields.statusReasons = requestMap.get("resource")?.get("reasonnotgiven")
        }

        if (requestMap.get("resource")?.get("effectiveTimePeriod")?.get("end")) {
            rasSegmentFields.endDateOfAdministration = requestMap.get("resource")?.get("effectiveTimePeriod")?.get("end")
        }

        if (requestMap.get("resource")?.get("effectiveTimePeriod")?.get("start")) {
            rasSegmentFields.startDateOfAdministration = requestMap.get("resource")?.get("effectiveTimePeriod")?.get("start")
        }
        if (requestMap.get("resource")?.get("effectiveTimePeriod")) {
            rasSegmentFields.effectivePeriod = requestMap.get("resource")?.get("effectiveTimePeriod")
        }

        if (requestMap.get("resource")?.get("requester")) {
            rasSegmentFields.medicationRequestRecorder = createHl7DTOFieldsFromPractitioner(requestMap, StringConstants.MEDICATION_PRESCRIBER, requestMap.get("resource")?.get("requester"))
        }

        if (requestMap.get("resource")?.get("medicationReference")) {
            rasSegmentFields = createHl7DTOFieldsFromMedication(requestMap, requestMap.get("resource")?.get("medicationReference"), rasSegmentFields)
        }

        if (rasSegments == null) {
            rasSegments = []
            rasSegments.add(rasSegmentFields)
        } else {
            rasSegments.add(rasSegmentFields)
        }
        return rasSegments
    }


    private Hl7Message createHl7DTOForGoal(Map requestMap, Hl7Message hl7Message) {
        List<GoalSegment> goalSegments = new ArrayList<>();
        if(hl7Message.goalSegments != null){
            goalSegments = hl7Message.goalSegments
        }

        GoalSegment goalSegment = new GoalSegment();

        if (requestMap.get("resource")?.get("target")) {
            goalSegment.goalTarget = requestMap.get("resource")?.get("target")
        }

        if (requestMap.get("resource")?.get("lifecycleStatus")) {
            goalSegment.goalLifecycleStatus = requestMap.get("resource")?.get("lifecycleStatus")?.toString()
        }

        if (requestMap.get("resource")?.get("identifier")) {
            goalSegment.goalIdentifier = athenaUtil.createIdentifierDTOs(requestMap.get("resource")?.get("identifier"), null, "usual")
        }

        if (requestMap.get("resource")?.get("description")) {
            goalSegment.goalDescription = athenaUtil.createCodeableConceptDTO(requestMap.get("resource")?.get("description"))
        }

        goalSegments.add(goalSegment)

        /*if (requestMap.get("entry")) {
            for (def entryObject in requestMap.get("entry")) {
                GoalSegment goalSegment = new GoalSegment();

                if (entryObject.get("resource")?.get("target")) {
                    goalSegment.goalTarget = entryObject.get("resource")?.get("target")
                }

                if (entryObject.get("resource")?.get("lifecycleStatus")) {
                    goalSegment.goalLifecycleStatus = entryObject.get("resource")?.get("lifecycleStatus")?.toString()
                }

                if (entryObject.get("resource")?.get("identifier")) {
                    goalSegment.goalIdentifier = athenaUtil.createIdentifierDTOs(entryObject.get("resource")?.get("identifier"))
                }

                if (entryObject.get("resource")?.get("description")) {
                    goalSegment.goalDescription = athenaUtil.createCodeableConceptDTO(entryObject.get("resource")?.get("description"))
                }

                goalSegments.add(goalSegment)
            }
        }*/


        hl7Message.goalSegments = goalSegments

        return hl7Message;

    }

    private Hl7Message createHl7DTOForORU(Map requestMap, Hl7Message hl7Message) {

        List categoryList = requestMap?.resource?.category
        String categoryDisplay = null

        categoryDisplay = requestMap?.resource?.category.getAt(0).coding.getAt(0)?.display

        if(categoryDisplay.equals("Radiology")){
            hl7Message.messageType = "ORU"
        }
        else{
            hl7Message.messageType = "ORU_LAB"
        }

        hl7Message.accession = requestMap?.resource?.id.toString().split("-")[2]
        hl7Message.orderNo = requestMap?.resource?.id.toString().split("-")[2]
        hl7Message.orderId = requestMap?.resource?.id.toString().split("-")[2]
        hl7Message.scheduledDate = requestMap?.resource?.issued
        if(requestMap?.resource?.effectiveDateTime){
            hl7Message.reportSignedDate =  athenaUtil.strToDateFormatTimeZoneConvert(requestMap?.resource?.effectiveDateTime, null, athenaConf?.timezone);
        }
        else {
            hl7Message.reportSignedDate =  athenaUtil.strToDateFormatTimeZoneConvert(requestMap?.resource?.issued, null, athenaConf?.timezone);
        }
        hl7Message.testCompendiumOrderId = requestMap?.resource?.code?.coding?.getAt(0)?.code ?: requestMap?.resource?.code?.text
        hl7Message.testCompendiumOrderSystem = requestMap?.resource?.code?.coding?.getAt(0)?.system
        hl7Message.appointmentDepartment = categoryDisplay
        hl7Message.reportStatus = athenaConf?.diagnosticReportStatus?.get(requestMap?.resource?.status)
        hl7Message.orderStatus = athenaConf?.diagnosticReportStatus?.get(requestMap?.resource?.status)
        hl7Message.messageBlob = createORUMessageBlob(hl7Message, requestMap);

        List<PractitionerMetaDataV2> practitionerList = new LinkedList<>();
        if(requestMap?.resource?.performer) {
            String[] pracIdList = requestMap?.resource?.performer.getAt(0)?.reference?.split("/");
            if(pracIdList?.size() == 2 && pracIdList[0].equals("Practitioner"))
            {
                practitionerList.addAll(createHl7DTOFieldsFromPractitioner(requestMap, StringConstants.ORDERER_DOCTOR, requestMap?.resource?.performer))
            }
        }

        hl7Message.practitionerList.addAll(practitionerList);
        return  hl7Message;
    }


    private List fetchReportObservations(def referenceEntry){

        List observationList = new LinkedList();


        for (def resultReference : referenceEntry?.resource?.result) {

            String[] observationIdList = resultReference?.reference?.split("/")
            String observationId;
            if (observationIdList?.size() == 2 && observationIdList[0].equals("Observation")) {
                observationId =  observationIdList[1]
            }
            if (observationId) {
                BasicDBObject observationSearchCriteria = new BasicDBObject("stagingId", referenceEntry?.stagingId).append("patientId", referenceEntry?.patientId).append("practiceId", referenceEntry?.practiceId).append("resource.identifier.value", observationId).append("apiName", "observationReportClinicalReference");
                Map observationMap = mongoService.search(observationSearchCriteria, athenaConst.REFERENCE_COLLECTION)
                if (observationMap.totalCount.count > 0) {
                    observationList.add(observationMap.objects.getAt(0));
                }
            }
        }
        return observationList

    }



    private String createORUMessageBlob(def hl7MessageObject, def referenceEntry){

        String messageBlob = null
       try{
        String categoryDisplay = referenceEntry?.resource?.category.getAt(0).coding.getAt(0)?.display
        HapiContext context = new DefaultHapiContext();
        ORU_R01 message = new ORU_R01();
        message.initQuickstart("ORU", "R01", "P");

        String accession = referenceEntry?.resource?.id.toString().split("-")[2]

        ORC orc = message.getPATIENT_RESULT().getORDER_OBSERVATION().getORC();
        orc.getOrderControl().setValue("NW");
        orc.getPlacerOrderNumber().getEntityIdentifier().setValue(accession);

        OBR obr = message.getPATIENT_RESULT().getORDER_OBSERVATION().getOBR();
        obr.getSetIDOBR().setValue("1");
        obr.getPlacerOrderNumber().getEntityIdentifier().setValue(accession);
        obr.getFillerOrderNumber().getEntityIdentifier().setValue(accession);
        obr.getUniversalServiceIdentifier().getIdentifier().setValue(referenceEntry?.resource?.code?.coding?.getAt(0)?.code?:referenceEntry?.resource?.code?.text);
        obr.getUniversalServiceIdentifier().getText().setValue(referenceEntry?.resource?.code?.coding?.getAt(0)?.display?:referenceEntry?.resource?.code?.text);
           String reportStatusStr = referenceEntry?.resource?.status
           if(athenaConf?.diagnosticReportStatus?.containsKey(referenceEntry?.resource?.status)){
               reportStatusStr = athenaConf?.diagnosticReportStatus?.get(reportStatusStr)

           }

           obr.getResultStatus().setValue(reportStatusStr)


           String issueDateStr = null
           if(referenceEntry?.resource?.issued){

               if(referenceEntry?.resource?.issued.toString().contains(".000Z")){
                   issueDateStr = referenceEntry?.resource?.issued.toString().replace(".000Z","")
               }
               issueDateStr = athenaUtil.strToStrFormatTimeZoneConvert(referenceEntry?.resource?.issued,"yyyy-MM-dd'T'HH:mm:ss","yyyyMMddHHmmss",null)
               if(issueDateStr){
                   obr.getObservationDateTime().getTime().setValue(issueDateStr);
                   obr.getResultsRptStatusChngDateTime().getTime().setValue(issueDateStr)
                   obr.getScheduledDateTime().getTime().setValue(issueDateStr);

               }

           }




           obr.getObr18_PlacerField1().setValue(accession)

        if(categoryDisplay != "Radiology"){
            List observationList = fetchReportObservations(referenceEntry)
            for(int i=0; i < observationList.size() ; i++){
                def obsObject = observationList[i]?.resource
                OBX obx = message.getPATIENT_RESULT().getORDER_OBSERVATION().getOBSERVATION(i).getOBX();
                obx.getSetIDOBX().setValue((i+1).toString());
                   obx.getValueType().setValue("NM");
                if(obsObject?.code?.coding && obsObject?.code?.coding.size() > 0 && obsObject?.code?.coding.getAt(0) && obsObject?.code?.coding.getAt(0)?.code)
                    obx.getObservationIdentifier().getIdentifier().setValue(obsObject?.code?.coding.getAt(0)?.code);
                else
                    obx.getObservationIdentifier().getIdentifier().setValue(obsObject?.code?.text);

                if(obsObject?.code?.coding && obsObject?.code?.coding.size() > 0 && obsObject?.code?.coding.getAt(0) && obsObject?.code?.coding.getAt(0)?.display)
                    obx.getObservationIdentifier().getText().setValue(obsObject?.code?.coding.getAt(0)?.display);
                else
                    obx.getObservationIdentifier().getText().setValue(obsObject?.code?.text);


                try{
                    if(obsObject?.valueString) {
                           String finalNote = obsObject?.valueString
                        obx.getObservationValue(0).getData().setValue(finalNote)
                    }
                       else if(obsObject?.valueQuantity){
                           if(obsObject?.valueQuantity?.value){
                               String valueStr = ""
                               NM value = new NM(message);

                               if(Double.valueOf(obsObject?.valueQuantity?.value.toString()).equals(0)){
                                   valueStr = "0"
                               }
                    else{
                                   valueStr = String.valueOf(obsObject?.valueQuantity?.value)
                               }
                               value.setValue(valueStr);
                               obx.getObservationValue(0).setData(value);
                    }
                           if(obsObject?.valueQuantity?.unit){
                               CE units = new CE(message);
                               units.getIdentifier().setValue(obsObject?.valueQuantity?.unit);
                               units.getText().setValue(obsObject?.valueQuantity?.unit);
                               obx.getUnits().getIdentifier().setValue(units.getIdentifier().getValue());
                               obx.getUnits().getText().setValue(units.getText().getValue());


                           }

                       }

                }
                catch(Exception ex){
                  log.error("Inside createORUMessageBlob : ",ex)
                }
                   if(obsObject?.referenceRange && obsObject?.referenceRange?.size() > 0){
                       String referenceRangeStr = obsObject?.referenceRange?.getAt(0)?.text

                       obx.getReferencesRange().setValue(referenceRangeStr);

                   }


                   if(obsObject?.interpretation && obsObject?.interpretation?.size() > 0){
                       if(obsObject?.interpretation?.getAt(0)?.coding?.getAt(0)?.code){
                           obx.getAbnormalFlags(0).setValue(obsObject?.interpretation?.getAt(0)?.coding?.getAt(0)?.code);
                       }
                   }

                if(obsObject?.status){
                    String observationStatusStr = obsObject?.status
                    if(athenaConf?.observationStatus?.containsKey(obsObject?.status)){
                        observationStatusStr = athenaConf?.observationStatus?.get(observationStatusStr)

                    }
                    obx.getObservationResultStatus().setValue(observationStatusStr)
                }


                if(obsObject?.effectiveDateTime){
                       String effectiveDate = athenaUtil.strToStrFormatTimeZoneConvert(obsObject?.effectiveDateTime,"yyyy-MM-dd'T'HH:mm:ss","yyyyMMddHHmmss",null)
                       if(effectiveDate)
                           obx.getDateTimeOfTheObservation().getTime().setValue(effectiveDate)
                }
                   else{
                    obx.getDateTimeOfTheObservation().getTime().setValue(referenceEntry?.resource?.issued);
                   }


                   String note = ""
                   if(obsObject?.note && obsObject?.note.size() > 0){
                       note = obsObject?.note[0].text.toString().replaceAll("(\r\n|\n)", "<br />")
                       for(int j=1;j<=obsObject?.note.size() ; j++){
                           if(obsObject?.note[j]?.text != "" && obsObject?.note[j]?.text != null){
                               note = note + obsObject?.note[j]?.text.toString().replaceAll("(\r\n|\n)", "<br />")
                           }
                       }
                   }
                   if(note != "" && note != null){
                       NTE nte = message.getPATIENT_RESULT().getORDER_OBSERVATION().getOBSERVATION(i).getNTE();
                       //NTE nte = obx.getNTE();
                       //NTE nte = obx.addNTERepetitionsUsed();
                       //nte.getSetIDNTE().setValue(1);
                       nte.getComment(0).setValue(note);
                   }






                //message.getPATIENT_RESULT().getORDER_OBSERVATION().getOBSERVATION(i).setOBX(obx);
            }
        }
        else{
            //Radiology
            if(referenceEntry?.resource?.presentedForm && referenceEntry?.resource?.presentedForm.size() > 0){
                for(int i=0;i<referenceEntry?.resource?.presentedForm.size();i++){
                    OBX obx = message.getPATIENT_RESULT().getORDER_OBSERVATION().getOBSERVATION(i).getOBX();
                    obx.getSetIDOBX().setValue((i+1).toString());
                    obx.getValueType().setValue("ED");
                    try {
                        if(referenceEntry?.resource?.presentedForm[i]?.contentType == "image/png"){
                            obx.getObservationValue(0).getData().setValue(athenaUtil.getBinaryData(referenceEntry?.resource?.presentedForm[i]?.url?.split("/")?.last(),referenceEntry?.practiceId))
                        }
                        else {
                            obx.getObservationValue(0).getData().setValue(referenceEntry?.resource?.presentedForm[i].data)
                        }
                    }
                    catch(Exception ex){
                        log.error("Exception :",ex)
                    }

                    //obx.getObservationValue(i).setData(referenceEntry?.resource?.presentedForm[i].data);


                    String observationStatusStr = referenceEntry?.resource?.status
                    if(athenaConf?.diagnosticReportStatus?.containsKey(referenceEntry?.resource?.status)){
                        observationStatusStr = athenaConf?.diagnosticReportStatus?.get(observationStatusStr)

                    }
                    obx.getObservationResultStatus().setValue(observationStatusStr);

                       if(referenceEntry?.resource?.issued){
                           String issueDateStr1 = athenaUtil.strToStrFormatTimeZoneConvert(referenceEntry?.resource?.issued,"yyyy-MM-dd'T'HH:mm:ss","yyyyMMddHHmmss",null)
                           if(issueDateStr1)
                               obx.getDateTimeOfTheObservation().getTime().setValue(issueDateStr1);
                       }

                }
            }



        }


        Parser parser = context.getPipeParser();
        messageBlob = parser.encode(message);
           if(messageBlob != null && categoryDisplay == "Radiology" && messageBlob.contains("ED|||")){
               messageBlob = messageBlob.replace("ED|||","ED|||^text/html^^base64^")
           }

            //log.debug("Inside createORUMessageBlob : "+messageBlob)
       }
        catch(Exception ex){
            log.error("Exception in createORUMessageBlob : ",ex)
        }
        return messageBlob
    }



    private Hl7Message createHl7DTOFieldsFromDocumentReference(Hl7Message hl7Message, Map requestMap){

        hl7Message.placerOrderNumber = requestMap?.resource?.identifier?.find{it.system == "https://fhir.athena.io/sid/ah-documentreference"}?.value?.split("-")?.last() ?: requestMap?.resource?.date?.replaceAll("[^0-9]","")
        hl7Message.documentType = requestMap?.resource?.resourceType?.equals("ClinicalImpression") ? "PR" : requestMap?.resource?.type?.coding?.getAt(0)?.display
        //hl7Message.securityLabel
        hl7Message.documentStatus = requestMap?.resource?.status
        if(requestMap?.resource?.date?.toString()?.contains("Z")){
            hl7Message.originationDate = athenaUtil.strToDateFormatTimeZoneConvert(requestMap?.resource?.date?.toString());
        }else {
            hl7Message.originationDate = athenaUtil.strToDateFormatTimeZoneConvert(requestMap?.resource?.date?.toString(), null, athenaConf?.timezone);
        }
        hl7Message.documentNumber = hl7Message.placerOrderNumber
        hl7Message.documentDescription = requestMap?.resource?.description
        if(requestMap?.resource?.content?.getAt(0)?.attachment?.data) {
            String fileName = "DocumentRef-" + hl7Message.placerOrderNumber + ".txt";
            hl7Message.documentFileName = fileName;
        }else if(requestMap?.resource?.content?.getAt(0)?.attachment?.contentType?.equals("image/png")){
            String fileName = "DocumentRefImg-" + hl7Message.placerOrderNumber + ".png";
            hl7Message.documentFileName = fileName;
        }
        hl7Message.parentDocumentNumber = hl7Message.documentNumber

        if(!requestMap?.resource?.content?.getAt(0)?.attachment?.extension?.getAt(0)?.url?.equals("http://hl7.org/fhir/StructureDefinition/data-absent-reason"))
            hl7Message.obxSegments = createObxSegmentFieldForClinicalOrDocumentReference(hl7Message.obxSegments, requestMap);
        List<PractitionerMetaDataV2> practitionerList = new LinkedList<>();
        if(requestMap?.resource?.author) {
            practitionerList.addAll(createHl7DTOFieldsFromPractitioner(requestMap, StringConstants.AUTHOR_DOCTOR, requestMap?.resource?.author))
        }
        if(requestMap?.resource?.assessor){
            practitionerList.addAll(createHl7DTOFieldsFromPractitioner(requestMap, StringConstants.PERFORMER_DOCTOR, requestMap?.resource?.assessor))
        }
        hl7Message.practitionerList.addAll(practitionerList);
        return  hl7Message;
    }

    List<ObxSegment> createObxSegmentFieldForClinicalOrDocumentReference(List<ObxSegment> obxSegments,Map requestMap){

        if(obxSegments == null){
            obxSegments = new LinkedList<>();
        }
        ObservationSegment obxSegment = new ObservationSegment();
        obxSegment.observationType = requestMap?.resource?.resourceType?.equals("DocumentReference") ? "ED" : "TR";
        if(obxSegment.observationType == StringConstants.OBSERVATION_TYPE_ED) {
            for(Map contentMap : requestMap?.resource?.content) {
            obxSegment.paramId = requestMap?.resource?.date ? requestMap?.resource?.date?.replaceAll(/-*T*:*\.*Z*/,"") : "Athena";
            obxSegment.paramName = "others";
                if(contentMap?.attachment?.contentType?.equals("image/png")){
                    obxSegment.resultValue = athenaUtil.getBinaryData(contentMap?.attachment?.url?.split("/")?.last(),requestMap?.practiceId)
                }else{
                    obxSegment.resultValue = contentMap?.attachment?.data;
                }
                obxSegment.valueType = contentMap?.attachment?.contentType;
                obxSegment.valueSubType = "";
            obxSegment.compositionResultValue = "";
                obxSegments.add(obxSegment)
            }
        } else {
            obxSegment.resultValue = "";
            obxSegment.valueType = "TEXT";
            obxSegment.valueFormat = "STRING"
            obxSegment.clinicalNoteType = "TEXT"
            obxSegment.clinicalNoteEncoding = "STRING"
            obxSegment.testResultStatus = "F"
            obxSegment.paramId = requestMap?.resource?.date ? requestMap?.resource?.date?.replaceAll(/-*T*:*\.*Z*/,"") : "Athena";
            obxSegment.paramName = "others";

        obxSegments.add(obxSegment)
        }

        String comment = requestMap?.resource?.summary;
        if (obxSegments?.last()?.comment == null) {
            obxSegments?.last()?.comment = "";
        }
        if (comment != null && !comment.isEmpty()) {
            obxSegments?.last()?.comment += comment;
        }
        return obxSegments;
    }

    private def createHl7DTOFieldsFromMedication(def clinicalObject, def medicationReference, def segmentFields){
        Map medicationMap = athenaUtil.fetchMedicationData(clinicalObject)

        if(medicationMap.totalCount.count>0){
            def medicationResource = medicationMap.objects.getAt(0)
            segmentFields.medicationName = medicationResource.get("resource")?.get("code")?.get("coding")?.getAt(0)?.get("display")
            segmentFields.medicationId = medicationResource.get("resource")?.get("code")?.get("coding")?.getAt(0)?.get("code")
        }
        return segmentFields
    }

    /* method for fetching historical data */

    private Map fetchHistoricalData(Map requestMap) {
        Map responseMap = new LinkedHashMap();
        responseMap.put("status", athenaConst.SUCCESS);
        log.info("Going to fetch the Historical data for apiName=>" + requestMap.get("apiName") + " jobInstanceNumber=>" + requestMap.get("jobInstanceNumber") + " limit=>" + requestMap.get("limit"))
        int totalcount = 1;
        while (totalcount > 0) {
            //Creating Search Map
            Map searchMap = createSearchMapForStaging(athenaConst.REFERENCE, requestMap);
            Map result = athenaUtil.fetchAthenaData(searchMap, true, athenaConst.PROCESSING_REFERENCES);
            if (result.get("isSuccess")) {
                log.info("fetchReferenceDataForStaging: Total Received Data: " + result.totalcount)
                totalcount = result.totalcount;
                Map apiConfig = athenaConst.getAPIConfig(athenaConst.PATIENT_REFERENCE);

                result.objects.each { doc ->
                    /* need to change logic for this */

                    String patientId = doc.appointmentCSVData?.patientid;
                    log.info("Going to fetch patient data for StagingID : "+doc._id+" | patientId : "+patientId)
                    Map updateMap = new LinkedHashMap();
                    boolean eligibleForIngest = true;

                    Map reqMap = ["departmentid": doc.departmentId, "practiceid": doc.practiceId, "apiName": athenaConst.PATIENT_REFERENCE, "ah-practice":"Organization/a-" + doc.departmentId + ".Practice-" + doc.practiceId, "patientid" : patientId];
                    Map resultMapForPatient = athenaUtil.athenaCallAPI(apiConfig, reqMap);

                    if (resultMapForPatient.status == athenaConst.SUCCESS) {
                        updateMap.put("retries", 0);
                        //Create record for reference collection
                        Map createMap = [:]
                        createMap.put("apiName",athenaConst.PATIENT_REFERENCE)
                        createMap.put("practiceId",doc.practiceId)
                        createMap.put("stagingId", doc._id)
                        createMap.put("jobInstanceNumber", requestMap.jobInstanceNumber)
                        createMap.put("limit", requestMap.limit)
                        createMap.put("patientId",patientId)
                        createMap.put("source","HISTORICAL")
                        Map resMapforPatient = athenaUtil.saveClinicalOrReferenceData(createMap, resultMapForPatient.get("result"))
                        if(!resMapforPatient.isSuccess) {
                            updateMap.put("status", athenaConst.FAILED);
                            updateMap.put("retries", doc.retries + 1);
                            updateMap.put("exceptionMsg", resMapforPatient.message);
                            eligibleForIngest = false

                        } else {
                            /* need to fetch encounter for this patient */
                            log.info("Going to fetch encounter data for StagingID : "+doc._id+" | patientId : "+patientId)
                            Map apiConfigForEncounter = athenaConst.getAPIConfig(athenaConst.ENCOUNTER_HISTORICAL_REFERENCE);
                            Map encounterReqMap = ["departmentid": doc.departmentId, "practiceid": doc.practiceId, "apiName": athenaConst.PATIENT_REFERENCE, "ah-practice":"Organization/a-" + doc.departmentId + ".Practice-" + doc.practiceId, "patientid" : patientId];
                            while(true) {
                                Map resultMapForEncounter = athenaUtil.athenaCallAPI(apiConfigForEncounter, encounterReqMap);

                                // create search map and
                                if (resultMapForEncounter.status == athenaConst.SUCCESS) {
                                    updateMap.put("retries", 0);
                                    Map createMapForEncounter = [:]
                                    createMapForEncounter.put("apiName", athenaConst.ENCOUNTER_REFERENCE)
                                    createMapForEncounter.put("practiceId", doc.practiceId)
                                    createMapForEncounter.put("stagingId", doc._id)
                                    createMapForEncounter.put("jobInstanceNumber", requestMap.jobInstanceNumber)
                                    createMapForEncounter.put("limit", requestMap.limit)
                                    createMapForEncounter.put("patientId", patientId)
                                    createMapForEncounter.put("source","HISTORICAL")
                                    Map resMapForEncounter = athenaUtil.saveClinicalOrReferenceData(createMapForEncounter, resultMapForEncounter.get("result"))
                                    if (!resMapForEncounter.isSuccess) {
                                        updateMap.put("status", athenaConst.FAILED);
                                        updateMap.put("retries", doc.retries + 1);
                                        updateMap.put("exceptionMsg", resMapForEncounter.message);
                                        eligibleForIngest = false
                                        break;
                                    } else {

                                    }
                                } else {
                                    updateMap.put("status", athenaConst.FAILED);
                                    updateMap.put("retries", doc.retries + 1);
                                    updateMap.put("exceptionMsg", resultMapForEncounter.message);
                                    eligibleForIngest = false
                                    break;
                                }


                                String nextUrl = resultMapForEncounter.get("result").get("link").find{it.relation.equals("next")}?.url
                                if(nextUrl && nextUrl != ""){
                                    apiConfigForEncounter.apiEndPoint = "/"+nextUrl.split(".com/")[1];
                                    log.info("Going to fetch next sequence of data with param:  "+ apiConfigForEncounter.apiEndPoint)
                                    apiConfigForEncounter.apiParams = [:]
                                    encounterReqMap = [:]
                                    continue;
                                }else{
                                    break;
                                }
                            }
                        }
                    } else {
                        updateMap.put("status", athenaConst.FAILED);
                        updateMap.put("retries", doc.retries + 1);
                        updateMap.put("exceptionMsg", resultMapForPatient.message);
                        eligibleForIngest = false
                    }
                    //Going to update the response with fetched Object
                    Map updateResult = null
                    if (eligibleForIngest) {
                        updateMap.put("status", athenaConst.PROCESSED)
                        updateResult = athenaUtil.updateStagingData(["searchCriteria": ["_id": doc._id], "updateData": updateMap])
                    }else {
                        updateResult = athenaUtil.updateStagingData(["searchCriteria": ["_id": doc._id], "updateData": updateMap])
                    }
                    if(!updateResult.isSuccess){
                        totalcount = 0;
                        log.error("fetchHistoricalData: => Error while updating the status. Aborting all other message for further processing");
                        responseMap.put("status", athenaConst.FAILURE);
                        responseMap.put("message", "Error while updating the status. Aborting all other message for further processing");
                    }

                }
            }else {
                totalcount = 0;
                log.error("fetchHistoricalData: => Error while fetching staging data.");
                responseMap.put("status", athenaConst.FAILURE);
                responseMap.put("message", result.get("message"));
            }

            if (athenaUtil.checkJobConfigStatus(requestMap.jobConfigId) == false) {
                log.info("fetchReferenceDataForStaging(): Exiting loop and stopping further processing as AthenaFetchReferenceDataService job is disabled.")
                break
            }
        }
        return responseMap;
    }

}



