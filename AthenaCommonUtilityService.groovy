package com.mphrx.core

import com.mongodb.DBObject
import com.mongodb.WriteResult
import com.mongodb.client.FindIterable
import com.mongodb.client.MongoCollection
import com.mphrx.dto.ConditionSegment
import com.mphrx.dto.HealthcareServiceSegment
import com.mphrx.dto.INSegment
import com.mphrx.dto.LocationSegment
import com.mphrx.dto.NK1Segment
import com.mphrx.dto.TelecommunicationNumber
import com.mphrx.dto.v2.components.CodeableConceptDTO
import com.mphrx.dto.v2.components.CodingDTO
import com.mphrx.dto.v2.components.HumanNameDTO
import com.mphrx.dto.v2.components.IdentifierDTO
import com.mphrx.dto.v2.components.PractitionerRoleDTOV2
import com.mphrx.util.calendar.DateUtils
import consus.basetypes.Address
import consus.basetypes.AddressMetadata
import consus.basetypes.ContactPoint
import consus.basetypes.ContactPointMetaData
import consus.basetypes.HumanNameMetaData
import consus.basetypes.Identifier
import consus.basetypes.IdentifierMetaData
import consus.basetypes.PractitionerMetaDataV2
import consus.constants.StringConstants
import consus.primitiveTypes.Code
import consus.primitiveTypes.StringType
import consus.resources.LocationResource
import grails.transaction.Transactional
import com.mphrx.util.grails.ApplicationContextUtil
import grails.util.Holders
import groovy.json.JsonOutput
import org.apache.log4j.Logger
//import org.slf4j.LoggerFactory
import com.mphrx.instrumentation.MyStopWatch
import grails.plugins.rest.client.RestBuilder
import org.bson.Document
import org.bson.conversions.Bson
import org.springframework.util.LinkedMultiValueMap
import org.springframework.util.MultiValueMap
import com.mongodb.BasicDBObject
import com.mongodb.BulkWriteOperation
import com.mongodb.WriteConcern
import org.springframework.http.HttpStatus
import com.mphrx.commons.consus.MongoService
import com.mphrx.dto.v2.components.AnnotationDTO
import java.text.DateFormat
import java.text.SimpleDateFormat
import com.mphrx.dto.v2.components.TelecomDTO
import com.mphrx.dto.v2.components.AddressDTO
import com.mongodb.BasicDBList
import com.mphrx.EncryptionUtils17
import com.mphrx.audit.ExternalAuditLog
import com.mphrx.dto.v2.components.IdentifierDTO
import com.mphrx.dto.v2.components.CodeableConceptDTO
import consus.basetypes.Coding
import consus.basetypes.CodeableConcept
import consus.basetypes.IdentifierMetaData
import com.mphrx.dto.ObservationSegment
import com.mphrx.consus.hl7.Hl7Message
import com.mphrx.dto.v2.ImmunizationDTO
import consus.basetypes.Quantity
import com.mphrx.dto.AL1Segment
import com.mphrx.dto.ReactionDTO
import com.mphrx.dto.PR1Segment
import com.mphrx.dto.v2.components.PeriodDTO
import org.bson.types.ObjectId
import com.mphrx.dto.v2.components.QuantityDTO
import org.json.JSONObject;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;


@Transactional
class AthenaCommonUtilityService {
    public static Logger log = Logger.getLogger("com.mphrx.AthenaCommonUtilityService")
    //Getting All beans object along with athena specific configs.
    def athenaConst = Holders.grailsApplication.mainContext.getBean("athenaConstantsAndAPIConfigService");
    def athenaConf = ApplicationContextUtil.getConfig().athena;
    private Map tokenMap = ["token":"", "defaultActiveTime":3300000, "lastRefreshTime": null, "maxRetry": 0, "qpsRetryCounter": 0];
    MongoService mongoService;

    Map defineMessageIdAndWorkFlow(String apiEndPoint){
        Map messageIdAndWorkflow = ["messageId":null,"workflowName":null]
        Long currentDate = new Date().time
        //TODO: mapper to be embedded based on the apiEndPoint, resulting with messageId and workflowName
        if(apiEndPoint == "/token"){
            messageIdAndWorkflow.messageId = "ATHENA_TOKEN_"+currentDate
            messageIdAndWorkflow.workflowName = "token"
        }
        if(apiEndPoint == "/v1/practiceId/patients/changed"){
            messageIdAndWorkflow.messageId = "ATHENA_PATIENTCHANGED_"+currentDate
            messageIdAndWorkflow.workflowName = "patientChangedData"
        }
        if(apiEndPoint == "/v1/practiceId/appointments/changed"){
            messageIdAndWorkflow.messageId = "ATHENA_APPOINTMENTCHANGED_"+currentDate
            messageIdAndWorkflow.workflowName = "appointmentChangedData"
        }
        if(apiEndPoint == "/fhir/r4/Patient"){
            messageIdAndWorkflow.messageId = "ATHENA_PATIENTREF_"+currentDate
            messageIdAndWorkflow.workflowName = "patientReference"
        }
        if(apiEndPoint == "/1/practiceinfo"){
            messageIdAndWorkflow.messageId = "ATHENA_PRACTICEINFO_"+currentDate
            messageIdAndWorkflow.workflowName = "practiceinfo"
        }
        if(apiEndPoint == "/v1/practiceId/departments"){
            messageIdAndWorkflow.messageId = "ATHENA_DEPARTMENTS_"+currentDate
            messageIdAndWorkflow.workflowName = "listDepartments"
        }
        if(apiEndPoint == "/v1/practiceId/practiceinfo"){
            messageIdAndWorkflow.messageId = "ATHENA_PRACTICEINFOLIST_"+currentDate
            messageIdAndWorkflow.workflowName = "practiceinfoList"
        }
        if(apiEndPoint == "/departments/departmentid"){
            messageIdAndWorkflow.messageId = "ATHENA_GETDEPARTMENT_"+currentDate
            messageIdAndWorkflow.workflowName = "getDepartment"
        }
        if(apiEndPoint.contains("/fhir/r4/Practitioner")){
            messageIdAndWorkflow.messageId = "ATHENA_PROVIDERREF_"+currentDate
            messageIdAndWorkflow.workflowName = "providerReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Encounter")){
            messageIdAndWorkflow.messageId = "ATHENA_ENCOUNTERREF_"+currentDate
            messageIdAndWorkflow.workflowName = "encounterReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Condition")){
            messageIdAndWorkflow.messageId = "ATHENA_CONDITIONREF_"+currentDate
            messageIdAndWorkflow.workflowName = "conditionClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/MedicationRequest")){
            messageIdAndWorkflow.messageId = "ATHENA_MEDICATIONREQUESTREF_"+currentDate
            messageIdAndWorkflow.workflowName = "medicationRequestClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/dstu2/MedicationAdministration")){
            messageIdAndWorkflow.messageId = "ATHENA_MEDICATIONADMINREF_"+currentDate
            messageIdAndWorkflow.workflowName = "medicationAdministrationClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/DiagnosticReport")){
            messageIdAndWorkflow.messageId = "ATHENA_DIAGNOSTICREPORTREF_"+currentDate
            messageIdAndWorkflow.workflowName = "diagnosticReportClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Observation")){
            messageIdAndWorkflow.messageId = "ATHENA_OBSERVATIONREF_"+currentDate
            messageIdAndWorkflow.workflowName = "observationClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/dstu2/ClinicalImpression")){
            messageIdAndWorkflow.messageId = "ATHENA_CLINICALIMPREF_"+currentDate
            messageIdAndWorkflow.workflowName = "clinicalImpressionClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/DocumentReference")){
            messageIdAndWorkflow.messageId = "ATHENA_DOCUMENTNREF_"+currentDate
            messageIdAndWorkflow.workflowName = "documentReferenceClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/ServiceRequest")){
            messageIdAndWorkflow.messageId = "ATHENA_SERVICEREQREF_"+currentDate
            messageIdAndWorkflow.workflowName = "serviceRequestClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Procedure")){
            messageIdAndWorkflow.messageId = "ATHENA_PROCEDUREREF_"+currentDate
            messageIdAndWorkflow.workflowName = "procedureClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Immunization")){
            messageIdAndWorkflow.messageId = "ATHENA_IMMUNIZATIONREF_"+currentDate
            messageIdAndWorkflow.workflowName = "immunizationClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/AllergyIntolerance")){
            messageIdAndWorkflow.messageId = "ATHENA_ALLERGYREF_"+currentDate
            messageIdAndWorkflow.workflowName = "allergyIntoleranceClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Goal")){
            messageIdAndWorkflow.messageId = "ATHENA_GOALREF_"+currentDate
            messageIdAndWorkflow.workflowName = "goalClinicalReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Medication")){
            messageIdAndWorkflow.messageId = "ATHENA_MEDICATIONREF_"+currentDate
            messageIdAndWorkflow.workflowName = "medicationReference"
        }
        if(apiEndPoint.contains("/fhir/r4/Binary")){
            messageIdAndWorkflow.messageId = "ATHENA_BINARYREF_"+currentDate
            messageIdAndWorkflow.workflowName = "binaryReference"
        }


        log.info("Inside defineMessageIdAndWorkFlow :" + messageIdAndWorkflow)
        return messageIdAndWorkflow
    }

    /**
     * @param apiConfig : API Config needs to call from AthenaConstantsAndAPIConfigService.getAPIConfig
     * @param requestMap
     * @return  Json Object : returned from API
     */
    public Map athenaCallAPI(Map apiConfig, Map requestMap){
        log.info("In athenaCallAPI for ApiEndPoint "+apiConfig.get("apiEndPoint")+". Token Config is =>"+tokenMap);
        boolean isValidToken = true;
        Map responseMap = new LinkedHashMap();


        try {
            Map messageIdAndWorkFlow = defineMessageIdAndWorkFlow(apiConfig.get("apiEndPoint"))
            //Check global token Map values and take action to get correct token
            if (tokenMap.get("token") != "" && tokenMap.get("lastRefreshTime")) {
                Long currentDateTime = new Date().getTime();
                Long tokenRefTime = tokenMap.get("lastRefreshTime").getTime() + Long.valueOf(tokenMap.get("defaultActiveTime"));;
                log.info("Token condition to match with next time tokenRefTime=> "+tokenRefTime+ "   currentDateTime=>"+currentDateTime);
                if (tokenRefTime <= currentDateTime) {
                    isValidToken = refreshBearerToken(messageIdAndWorkFlow);
                } else if (athenaConf.tokenRetry < tokenMap.get("maxRetry")) {
                    isValidToken = false;
                    Date newTokenEnableDate = new Date(tokenMap.get("lastRefreshTime").getTime() + tokenMap.get("defaultActiveTime")
                            + athenaConf.tokenRetryThresholdInterval);
                    log.error("Maximum threshold reached to call the Token API. Will be enable after =>" + newTokenEnableDate);
                }
            } else {
                isValidToken = refreshBearerToken(messageIdAndWorkFlow);
            }
            if (log.isDebugEnabled())
                log.debug("Token after token condition check " + tokenMap);

            //Calling the respactive API as per apiConfig
            if (isValidToken) {
                Map apiParameters = createApiParameters(apiConfig, requestMap);
                def responseObj = null;
                boolean isValidApi = true;
                if (apiConfig.get("apiMethod") == athenaConst.GET && !(requestMap.containsKey("getSubscription") && requestMap.get("getSubscription"))) {
                    responseObj = callGetAPI(apiParameters,messageIdAndWorkFlow);
                } else if (apiConfig.get("apiMethod") == athenaConst.POST || (requestMap.containsKey("getSubscription") && requestMap.get("getSubscription"))) {
                    responseObj = callPostAPI(apiParameters,false,messageIdAndWorkFlow);
                } else {
                    log.error("Defined method [${apiConfig.get("apiMethod")}] in config is not valid. Aborting the further execution");
                    isValidApi = false;
                    responseMap.put("status", athenaConst.FAILURE);
                    responseMap.put("result", [:])
                    responseMap.put("message", athenaConst.INVALID_METHOD);
                }

                if (responseObj?.statusCode == HttpStatus.TOO_MANY_REQUESTS){
                    if(requestMap?.get("apiName")?.equals("patientReference") || requestMap?.get("apiName")?.equals("encounterReference")){
                        mongoService.update(new BasicDBObject("serviceName",new BasicDBObject("\$in",["athenaAppointmentHistoricalDataCSVRead","athenaFetchReferenceData"])), new BasicDBObject("\$set", new BasicDBObject("enabled",false)), "jobConfiguration",false, true)
                    }
                    else{
                        mongoService.update(new BasicDBObject("serviceName","athenaFetchClinicalReferenceData"), new BasicDBObject("\$set", new BasicDBObject("enabled",false)), "jobConfiguration",false, true)
                    }
                }

                //Parsing the received response
                if(isValidApi){
                    try {
                        if (responseObj && responseObj?.statusCode == HttpStatus.OK && responseObj?.body != null
                                && responseObj?.body != "" && responseObj?.body != "null" && responseObj?.body != "NULL") {
                            responseMap.put("status", athenaConst.SUCCESS);
                            responseMap.put("result", Document.parse(responseObj.json.toString()));
                            responseMap.result.apiUrl = apiParameters?.restUrl
                            tokenMap.put("qpsRetryCounter", 0);
                        } else if (responseObj && responseObj?.statusCode == HttpStatus.UNAUTHORIZED) {
                            responseMap.put("status", athenaConst.FAILURE);
                            responseMap.put("result", [:])
                            responseMap.put("message", athenaConst.INVALID_TOKEN);
                        } else if (responseObj && tokenMap.get("qpsRetryCounter") == 0 && responseObj?.statusCode == HttpStatus.FORBIDDEN
                                && responseObj.body && responseObj.body.toLowerCase().contains("qps")) {
                            //TODO put a retry limit on qpr retries
                            tokenMap.put("qpsRetryCounter", 1);
                            log.info("QPS limit reached, API Response : ${responseObj.body}. Invoking sleep for 1 second.");
                            return callAthenaApiAfterDelay(apiConfig, requestMap);
                        } else {
                            log.error("Unknow Error Occurred for API [${apiConfig.get("apiEndPoint")}]. " +
                                    "Token Status =>" + tokenMap + ". responseCode : ${responseObj?.statusCode?.value()} , responseObj.body : ${responseObj?.body}");
                            responseMap.put("status", athenaConst.FAILURE);
                            responseMap.put("result", [:])
                            if (responseObj && responseObj?.statusCode) {
                                String exceptionMsg = "responseCode : " + responseObj.statusCode.value() + " , responseObj.body : " + responseObj?.body
                                responseMap.put("message", exceptionMsg);
                            } else
                                responseMap.put("message", "Some Exception occurred. Please check the other logs");
                        }
                    } catch (Exception e) {
                        log.error("athenaCallAPI(): Exception occurred while parsing Athena api response object - ${responseObj} - "+e.getMessage(), ex)
                    }
                }

            } else {
                log.error("Not able to call the REST-API due to invalid token. API is [${apiConfig.get("apiEndPoint")}]. Token Status =>" + tokenMap);
                responseMap.put("status", athenaConst.FAILURE);
                responseMap.put("result", [:])
                responseMap.put("message", athenaConst.INVALID_TOKEN);
            }
        }catch (Exception ex){
            log.error("athenaCallAPI-> exception occur:"+ex.getMessage(), ex)
            responseMap.put("status", athenaConst.FAILURE);
            responseMap.put("result", [:])
            responseMap.put("message", ex.getMessage());
        }

        return responseMap;
    }

    private def callAthenaApiAfterDelay(Map apiConfig, Map requestMap) {
        Thread.sleep(athenaConf.QPSDelayInSeconds * 1000);
        log.info("Calling the athenaCallAPI again post sleep.");
        Map responseMap = athenaCallAPI(apiConfig, requestMap);
        log.debug("Response map after callAthenaApiAfterDelay: "+responseMap);
        return responseMap;
    }

    /**
     *
     * @param apiParameters : All required parameter to call the API
     * @param isToken : Default false. Only used to get the bearer token using Oauth
     * @return  Json Object : For Subscription and Search API HashMap while For Others as JSON Array
     */

    private def callPostAPI(Map apiParameters, boolean isToken = false,Map messageIdAndWorkFlow) {
        if (log.isDebugEnabled())
            log.debug("Inside callPostAPI , apiParameters : ${apiParameters}")
        MyStopWatch myStopWatch = new MyStopWatch(apiParameters?.stopWatchMethodName)
        def responseObj = null;
        String method = "POST"
        def externalAuditResponseObject = null
        try {
            log.info("Inside callPostAPI with URL : "+apiParameters?.restUrl)
            log.info("Inside callPostAPI with requestBody : "+apiParameters?.params)
            myStopWatch.start(apiParameters?.stopWatchStartString)
            RestBuilder rest = new RestBuilder();
            responseObj = rest.post(apiParameters?.restUrl) {
                if (isToken) {
//                    JSONObject secretValueJson = getSecret(athenaConf.athenaSecretName)
//                    auth(secretValueJson.getString("clientId"), secretValueJson.getString("secret"));
                    auth(EncryptionUtils17.decrypt(athenaConf.clientId), EncryptionUtils17.decrypt(athenaConf.clientSecretId));
                }else {
                    header("Authorization", "Bearer "+tokenMap.get("token"));
                }
                accept("application/json")
                contentType("application/x-www-form-urlencoded");
                body(apiParameters?.params)
            }
            //TODO: A response manager and feeder method should be created to frame successful and exeception responses
            if(responseObj?.statusCode == HttpStatus.OK && responseObj?.body != null && responseObj?.body != "" && responseObj?.body != "null" && responseObj?.body != "NULL"){
                externalAuditResponseObject = responseObj
            }
            else if(responseObj?.statusCode && responseObj?.statusCode != HttpStatus.OK && responseObj?.body != null){
                externalAuditResponseObject = ["statusCode":responseObj?.statusCode,"body" :responseObj?.body.toString()]
            }
            else {
                externalAuditResponseObject = ["statusCode":500,"body" :"Error - Facing Connection timeout with Athena API"]
            }

            log.info("Inside callPostAPI received response statusCode: "+responseObj?.statusCode )
        } catch (Exception e) {
            log.error("Exception in callPostAPI Not able to do post call : ", e)
        }finally{
            myStopWatch.stop()
            myStopWatch.prettyPrint()
            externalAudit(externalAuditResponseObject, apiParameters, myStopWatch?.getTotalTimeMillis().toString(), method, messageIdAndWorkFlow.workflowName, messageIdAndWorkFlow.messageId)
        }
        return responseObj;
    }


    /**
     * @param apiParameters : All required parameter to call the API
     * @return  Json Object : For Subscription and Search API HashMap while For Others as JSON Array
     */
    private def callGetAPI(Map apiParameters, Map messageIdAndWorkFlow) {
        if (log.isDebugEnabled())
            log.debug("Inside callGetAPI , apiParameters : ${apiParameters}")
        MyStopWatch myStopWatch = new MyStopWatch(apiParameters?.stopWatchMethodName)
        def responseObj = null;
        String method = "GET"
        def externalAuditResponseObject = null
        try {
            log.info("Inside callGetAPI with URL : "+apiParameters?.restUrl)
            myStopWatch.start(apiParameters?.stopWatchStartString)
            def rest = new RestBuilder();
            responseObj = rest.get(apiParameters?.restUrl) {
                header("Authorization", "Bearer "+tokenMap.get("token"));
                accept("application/json")
                //contentType("application/x-www-form-urlencoded");
                //urlVariables(apiParameters?.params)
            }
            if(responseObj?.statusCode == HttpStatus.OK && responseObj?.body != null && responseObj?.body != "" && responseObj?.body != "null" && responseObj?.body != "NULL"){
                externalAuditResponseObject = responseObj
            }
            else if(responseObj?.statusCode && responseObj?.statusCode != HttpStatus.OK && responseObj?.body != null){
                externalAuditResponseObject = ["statusCode":responseObj?.statusCode,"body" :responseObj?.body.toString()]
            }
            else {
                externalAuditResponseObject = ["statusCode":500,"body" :"Error - Facing Connection timeout with Athena API"]
            }

            log.info("Inside callGetAPI received response statusCode: "+responseObj?.statusCode )
        } catch (Exception e) {
            log.error("Exception in callGetAPI - Not able to do get call for apiParameteres - ${apiParameters} | exception is : ", e)
        }finally{
            myStopWatch.stop()
            myStopWatch.prettyPrint()
            externalAudit(externalAuditResponseObject, apiParameters, myStopWatch?.getTotalTimeMillis().toString(), method, messageIdAndWorkFlow.workflowName, messageIdAndWorkFlow.messageId)
        }
        return responseObj;
    }

    /**
     *
     * @return
     */
    private boolean refreshBearerToken(Map messageIdAndWorkFlow){
        boolean isValidToken = false;
        if(athenaConf.tokenRetry > tokenMap.get("maxRetry")) {
            Map tokenApiConfig = athenaConst.getAPIConfig(athenaConst.TOKEN_API);
            Map apiParameter = new LinkedHashMap();
            apiParameter.put("stopWatchMethodName", "AthenaCommonUtilityService.refreshBearerToken()");
            apiParameter.put("stopWatchStartString", "Athena POST API to get Bearer Token");
            apiParameter.put("restUrl", athenaConf.apiUrl + athenaConf.tokenVersion + tokenApiConfig.get("apiEndPoint"));
            MultiValueMap<String, String> form = new LinkedMultiValueMap<String, String>();
            form.add("grant_type","client_credentials");
            //TODO "scope" to be moved as a configuration for further api extensions
            form.add("scope","system/AllergyIntolerance.read system/Binary.read system/CarePlan.read system/CareTeam.read system/Condition.read system/Device.read system/DiagnosticReport.read system/DocumentReference.read system/Encounter.read system/Goal.read system/Immunization.read system/Location.read system/Medication.read system/MedicationRequest.read system/Observation.read system/Organization.read system/Patient.read system/Practitioner.read system/PractitionerRole.read system/Procedure.read system/Provenance.read system/ServiceRequest.read athena/service/Athenanet.MDP.*");
            apiParameter.put("params", form);
            Date currentDate = new Date();
            def responseObj = callPostAPI(apiParameter, true,messageIdAndWorkFlow);
            //TODO: The method created after callPostAPI can be utilised for the validated response, then the below checks can be avoided
            if (responseObj && responseObj?.statusCode == HttpStatus.OK && responseObj?.body != null && responseObj?.body != ""
                    && responseObj?.body != "null" && responseObj?.body != "NULL") {
                def responseJson = responseObj.json;
                tokenMap.put("token", responseJson.access_token);
                tokenMap.put("defaultActiveTime", Long.valueOf(responseJson.expires_in).longValue() * 1000);
                tokenMap.put("lastRefreshTime", currentDate);
                tokenMap.put("maxRetry", 0);
                isValidToken = true;
            } else {
                log.error("Error while creating Bearer Token. responseCode : ${responseObj?.statusCode?.value()} , responseObj.body : ${responseObj?.body}");
                tokenMap.put("maxRetry", tokenMap.get("maxRetry") + 1);
            }
        }else {
            Long currentDateInt = new Date().getTime();
            Date lastRefreshDateTime = new Date(currentDateInt - athenaConf.tokenRetryThresholdInterval - tokenMap.get("defaultActiveTime"))
            tokenMap.put("lastRefreshTime", lastRefreshDateTime);
            tokenMap.put("maxRetry", 0);
            log.error("refreshBearerToken : Max retry reached to get the bearer token. Will retry after [${athenaConf.tokenRetryThresholdInterval}] Milli Seconds")
        }
        return isValidToken;
    }

    public def externalAudit(def responseOut, Map apiParams, String timeInMilli, String method, String workFlow, String messageIdentifier, String apiURL = null) {
        log.debug("Inside externalAudit, response :" + responseOut + " apiParams :" + apiParams + " time in milli :" + timeInMilli + " api method :" + method + " workflow Name :" + workFlow + " requestIdentifier" + messageIdentifier)
        ExternalAuditLog externalAuditLog = new ExternalAuditLog()
        //TODO : Map Initialization
        Map requestMap = [:]
        requestMap.put("uri", ((apiURL != null) ? apiURL : apiParams?.restUrl))
        requestMap.put("method", method)
        externalAuditLog.auditTrailIdentifier = 0  // Not applicable for CA
        if (apiParams && apiParams?.params != null) {
            def output = JsonOutput.toJson(apiParams?.params)
            requestMap.put("requestPayload", output)
        } else {
            requestMap.put("requestPayload", "")
        }
        //requestMap.put("headers", ["Authorization": ((basicHeader != null) ? basicHeader : ""), "Content-Type": "application/json", "Accept": "application/json"])
        requestMap.put("requestContentType", "application/json")
        externalAuditLog.requestMap = requestMap
        externalAuditLog.requestIdentifier = messageIdentifier

        externalAuditLog.dateTimeStamp = new Date()
        externalAuditLog.workflowName = workFlow


        //TODO : Map Initialization
        Map responseMap = [:]

        Integer apiStatusCode = null
        if (responseOut?.statusCode instanceof org.springframework.http.HttpStatus) {
            apiStatusCode = responseOut?.statusCode.value()
        } else {
            apiStatusCode = responseOut?.statusCode
        }
        responseMap.put("status", apiStatusCode)
        String output = ""
        if (responseOut instanceof Map) {
            output = JsonOutput.toJson(responseOut?.body)
        } else {
            output = JsonOutput.toJson(responseOut?.json)
        }
        responseMap.put("responsePayload", output)
        responseMap.put("headers", ["Date": responseOut?.headers?.Date, "Content-Type": "application/json", "Connection": responseOut?.headers?.Connection])
        responseMap.put("responseContentType", "application/json")
        externalAuditLog.responseMap = responseMap
        if (timeInMilli) {
            Integer timeInMilliSec = Integer.valueOf(timeInMilli)
            externalAuditLog.roundTripTime = timeInMilliSec
        }
        externalAuditLog.logMessage = "Request_End"
        try {
            log.info("Going to save externalAuditLog for workflow :" + workFlow + " , messageIdentifier : " + messageIdentifier)
            externalAuditLog.save(failOnError: true)
        }
        catch (Exception ex) {
            log.error("Exception on saving externalAuditLog :", ex)
        }

    }


    /**
     *
     * @param apiConfig : apiConfig with all parameters
     * @param requestMap : Source requestMap. It should contains the practiceid with other related field defined in API config.
     * @return
     */
    public Map createApiParameters(Map apiConfig, Map requestMap){
        Map apiParameters = new LinkedHashMap();
        if(apiConfig.get("apiEndPoint").contains("r4")){
            if(requestMap.apiName.equals("patientReference")){
                requestMap[apiConfig.get("primaryIdKey")] = "a-"+requestMap.practiceid+".E-"+requestMap.get(apiConfig.get("primaryIdKey"));
            }else if(requestMap.apiName.equals("encounterReference")){
                requestMap[apiConfig.get("primaryIdKey")] = "a-"+requestMap.practiceid+".encounter-"+requestMap.get(apiConfig.get("primaryIdKey"));
            }else if(requestMap.apiName.equals("providerReference")){
                requestMap[apiConfig.get("primaryIdKey")] = "a-"+requestMap.practiceid+".Provider-"+requestMap.get(apiConfig.get("primaryIdKey"));
            }
        }
        MultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>();
        String restUrl = athenaConf.apiUrl;
        restUrl = restUrl+apiConfig.get("apiEndPoint");
        String stopWatchMethodName = "AthenaCommonUtilityService.callAPI().createApiParameters()";
        String stopWatchStartString = "Athena "+apiConfig.get("apiMethod")+" API to get "+apiConfig.get("apiEndPoint")+" data. Api Type:"+apiConfig.get("apiType");
        String getParams = "";
        //Creating URL by replacing the reference data from objects along with static data
        if(apiConfig.apiParams && apiConfig.apiParams.size()>0){
            for(String paramKey : apiConfig.apiParams.keySet()){
                for(String aliasVal : apiConfig.apiParams.get(paramKey)){
                    if(requestMap.containsKey(aliasVal)){
                        if(getParams != "")
                            getParams = getParams+"&";
                        //restUrl = restUrl.replaceAll(aliasVal, requestMap.get(aliasVal).toString());
                        //apiConfig.apiParams[paramKey] = requestMap.get(aliasVal).toString();
                        getParams = getParams+paramKey+"="+requestMap.get(aliasVal).toString()
                        break;
                    }
                }
            }
        }
        requestMap.each{key,value ->
            restUrl = restUrl.replaceAll(key, value.toString());
        }

        //Checking and adding the department id for subscription APIs
        if((apiConfig.get("apiType") == athenaConst.SUBSCRIPTION || apiConfig.get("apiType") == athenaConst.SEARCH) &&
                requestMap.containsKey("departmentid") && requestMap.get("departmentid") != ""){
            getParams = "departmentid="+requestMap.get("departmentid").toString();
        }

        //TODO Add support for date range related additional parameters using field startDate  and endDate

        if(apiConfig.apiStaticParams && apiConfig.apiStaticParams.size() > 0) {
            for (String paramKey : apiConfig.apiStaticParams.keySet()) {
                if(!restUrl.contains(paramKey)) {
                if(getParams != "")
                    getParams = getParams+"&";
                getParams = getParams+paramKey+"="+apiConfig.apiStaticParams.get(paramKey).toString();
                params.add(paramKey, apiConfig.apiStaticParams.get(paramKey).toString());
            }
        }
        }

        //Checking subscription API and enable them as per request. We will append subscription at the end of the native API.
        if((requestMap.containsKey("checkSubscription") && requestMap.get("checkSubscription")) ||
                (requestMap.containsKey("getSubscription") && requestMap.get("getSubscription"))){
            restUrl = restUrl+ "/subscription"
        }//Appending URL parameters if api method is GET
        else if(getParams != "" && apiConfig.get("apiMethod") == athenaConst.GET){
            if(restUrl?.contains("cursor")){
                restUrl = restUrl + "&" + getParams;
            }else {
                restUrl = restUrl + "?" + getParams;
            }
            if(apiConfig.get("_include") != null){
                restUrl = restUrl + "&_include="+apiConfig.get("_include")
            }
        }

        //Creating API Parameters
        apiParameters.put("params",params);
        apiParameters.put("restUrl",restUrl)
        apiParameters.put("stopWatchMethodName",stopWatchMethodName)
        apiParameters.put("stopWatchStartString",stopWatchStartString)

        log.info("Final URL apiParameters is ===>"+apiParameters);
        return apiParameters;
    }
    public Map getToken(){
        return tokenMap;
    }

    /**
     *
     * @param apiConfig
     * @param requestMap
     * @return
     * requestMap = ["modField":"patientDob or any common date","data":"Single Json Object", "practiceId":"practiceId","departmentId":"departmentId",
     * "apiName":"API Key from ApiConfig"]
     */
    public Map saveStagingData(Map apiConfig, Map requestMap){
        if (log.isDebugEnabled())
            log.debug("saveStagingData() : requestMap received is -> ${requestMap}")
        Map resultMap = [:];
        try {
            //Creating the document structure going to be saved
            Date currentDate = new Date();
            DBObject athenaDocument = new BasicDBObject();
            athenaDocument.put("jobInstanceNumber", getJobInstanceNumber(requestMap.get("modField"),null, requestMap.get("apiName") ));
            athenaDocument.put("modField", requestMap.get("modField"));
            athenaDocument.put("retries", 0);
            athenaDocument.put("status", athenaConst.INITIATED);
            athenaDocument.put("dateCreated", currentDate);
            athenaDocument.put("lastUpdated", currentDate);
            athenaDocument.put("messageType", apiConfig.get("messageType"));
            println("============level 1")
            if (apiConfig.get("otherReferenceApis") && apiConfig.get("otherReferenceApis").size() > 0) {
                Map refApiMap = apiConfig.get("otherReferenceApis");
                Map finalValMap = new LinkedHashMap();
                refApiMap.each { k, v ->
                    Map valueMap = new LinkedHashMap();
                    valueMap.put("keyName", v.get("keyName"));
                    valueMap.put("type", v.get("type"));
                    if (requestMap.get("data").containsKey(v.get("keyName"))) {
                        valueMap.put("value", requestMap.get("data").get(v.get("keyName")))
                    }
                    finalValMap.put(k, valueMap);
                }
                athenaDocument.put("otherReferenceApis", finalValMap);
            } else {
                athenaDocument.put("otherReferenceApis", [:]);
            }
            athenaDocument.put("practiceId", requestMap.get("practiceId"));
            athenaDocument.put("departmentId", (requestMap.get("departmentId")) ?: "");
            athenaDocument.put("metadataList", createOrUpdateMetadata(requestMap, apiConfig.get("primaryIdKey")));
            athenaDocument.put("apiName", requestMap.get("apiName"));
            athenaDocument.put("apiType", apiConfig.get("apiType"));
            athenaDocument.put("sourceOfOrigin","CDC");
            athenaDocument.put(requestMap.get("apiName"), requestMap.get("data"));
            //println("============level 2")
            if(requestMap.get(athenaConst.DEPARTMENT_REFERENCE)) {
                athenaDocument.put(athenaConst.DEPARTMENT_REFERENCE, requestMap.get(athenaConst.DEPARTMENT_REFERENCE));
                athenaDocument.put("metadataList",createOrUpdateMetadata(["data": requestMap.get(athenaConst.DEPARTMENT_REFERENCE), "apiName":athenaConst.DEPARTMENT_REFERENCE], athenaConst.getAPIConfig(athenaConst.DEPARTMENT_REFERENCE).get("primaryIdKey"), athenaDocument.get("metadataList")));
            }
            //Going to insert the document
            //println("============level 3")
            List docList = new ArrayList()
            docList.add(athenaDocument)
            WriteResult result = mongoService.collection(athenaConst.STAGING_COLLECTION).insertOne(athenaDocument as Document);
            resultMap.put("isSuccess",true);
            resultMap.put("mongoResult",result);

        }catch (Exception ex){
            resultMap.put("isSuccess",false);
            resultMap.put("message",ex.getMessage());
            log.error("saveStagingData : Not able to ingest data in staging collection. Request map is - ${requestMap} =>"+ex.getMessage(), ex);
        }finally{

        }
        return resultMap;
    }

    /**
     *
     * @param requestMap  : Map Required as ["searchCriteria": "Map(Inner Filed can have BasicDBObject)", "limit": 10, "offset": 0, "sortCriteria" : "Map of fields", "selectedFields": "Map of required fields" ]
     * @param isTotalCountRequired : If total count needed along with search result
     * @param updateProcessingStatus : Need status update if required for processing data. This will be done through bulkUpdate.
     * @return : Success/Failure result in Map
     */
    public Map fetchAthenaData(Map requestMap, boolean isTotalCountRequired = false, String updateProcessingStatus){
        Map resultMap = [:];
        List<DBObject> objects = []
        String messageType = requestMap.get("messageType")
        def cursor = null
        if (log.isDebugEnabled())
            log.debug("fetchAthenaData()=> Received requestMap "+requestMap);
        try{
            if(requestMap.containsKey("searchCriteria") && requestMap.get("searchCriteria") instanceof Map && requestMap.get("searchCriteria").size()>0) {
                DBObject searchCriteria = new BasicDBObject(requestMap.get("searchCriteria"));
                DBObject sortCriteria = null;
                DBObject selectedFields = null;
                //BulkWriteOperation bulkWriteOperation = (updateProcessingStatus != "")? mongoService.collection(requestMap.get("athenaCollection")).initializeUnorderedBulkOp() : null;
                MongoCollection<Document> athenaCollection = mongoService.collection(requestMap.get("athenaCollection"));
                FindIterable<Document> iterable;
                int limit = (requestMap.containsKey("limit")) ? requestMap.get("limit") : 100;
                int offset = (requestMap.containsKey("offset")) ? requestMap.get("offset") : 0;


                if (requestMap.containsKey("sortCriteria") && requestMap.get("sortCriteria") instanceof Map && requestMap.get("sortCriteria").size() > 0) {
                    sortCriteria = new BasicDBObject(requestMap.get("sortCriteria"));
                } else {
                    sortCriteria = new BasicDBObject("_id", 1);
                }
                if (requestMap.containsKey("selectedFields") && requestMap.get("selectedFields") instanceof Map && requestMap.get("selectedFields").size() > 0) {
                    selectedFields = new BasicDBObject(requestMap.get("selectedFields"));
                }
                log.info("searchCriteria :"+searchCriteria);
                if (selectedFields) {
                    cursor = athenaCollection.find(searchCriteria, selectedFields).skip(offset).sort(sortCriteria).limit(limit).iterator();
                }
                else {
                    cursor = athenaCollection.find(searchCriteria).skip(offset).sort(sortCriteria).limit(limit).iterator();
                    //cursor = athenaCollection.find(new BasicDBObject("_id",123)).iterator();
                }

                int count =0;
                //Iterator<DBObject> iterator = cursor.iterator()
                while (cursor.hasNext()) {
                    Document obj = cursor.next();
                    objects.add(obj)
                    count++;
                    if(updateProcessingStatus != ""){
                        if (messageType && messageType != "" && ["encounterReference","patientReference"].contains(searchCriteria?.apiName)){
                            BasicDBObject updateCriteria = new BasicDBObject("_id", obj._id).append("hl7CreationStatusMap.messageType",messageType);
                            BasicDBObject setValues = new BasicDBObject("hl7CreationStatusMap.\$.status",updateProcessingStatus);
                            setValues.put("lastUpdated", new Date());
                            setValues.put("hl7CreationLockDate."+messageType,new Date());
                            BasicDBObject updateValueSet = new BasicDBObject('$set',setValues);
                            athenaCollection.updateOne(updateCriteria,updateValueSet);
                        } else if(requestMap.get("updateData")) {
                            log.debug("Reading Encounter Data")
                        } else {
                            BasicDBObject updateCriteria = new BasicDBObject("_id", obj._id);
                            BasicDBObject setValues = new BasicDBObject("status",updateProcessingStatus);
                            setValues.put("lastUpdated", new Date());
                            setValues.put("referenceLockDate",new Date());
                            BasicDBObject updateValueSet = new BasicDBObject('$set',setValues);
                            athenaCollection.updateOne(updateCriteria,updateValueSet);
                        }

                    }
                }
                if (isTotalCountRequired) {
                    resultMap.put("totalcount",count);//athenaCollection.getCount(,searchCriteria, long limit, long skip));
                }
                resultMap.put("objects", objects);
                /*if(updateProcessingStatus != "" && objects.size() >0){
                    bulkWriteOperation.execute(WriteConcern.SAFE);
                }*/
                resultMap.put("isSuccess", true);
            }else {
                log.error("fetchAthenaData : Not able to fetch data due to missing/misconfigured searchCriteria. It should be Map. Received requestMap: "+requestMap);
                resultMap.put("isSuccess", false);
                resultMap.put("message", "missing or misconfigured searchCriteria");
            }
        }catch (Exception ex){
            resultMap.put("isSuccess",false);
            resultMap.put("message",ex.getMessage());
            log.error("fetchAthenaData : Not able to fetch data from ${requestMap.get("athenaCollection")} collection for requestMap=>"+requestMap+" Exception:"+ex.getMessage(), ex);
        }finally{
            if(cursor){
                cursor?.close();
            }
        }
        return resultMap;
    }

    /**
     *
     * @param requestMap : Map required as ["searchCriteria": "Map of fields", "updateData": "Update Map Data with fields", "upsert": true, "multiUpdate": true]
     * @return Success/Failure result in Map
     */
    public Map updateStagingData(Map requestMap){
        Map resultMap = [:];
        try{
            if(requestMap.containsKey("searchCriteria") && requestMap.get("searchCriteria") instanceof Map && requestMap.get("searchCriteria").size()>0
                    && requestMap.containsKey("updateData") && requestMap.get("updateData") instanceof Map && requestMap.get("updateData").size()>0 ) {
                DBObject searchCriteria = new BasicDBObject(requestMap.get("searchCriteria"));
                boolean upsert = (requestMap.containsKey("upsert")) ? requestMap.get("upsert") : false
                boolean multiUpdate = (requestMap.containsKey("multiUpdate")) ? requestMap.get("multiUpdate") : false
                Date currentDate = new Date();
                Map updateDataTmp = requestMap.get("updateData")
                updateDataTmp.put("lastUpdated",currentDate)
                if(updateDataTmp.get("status") == athenaConst.PROCESSED){
                    updateDataTmp.put("referenceProcessedDate",currentDate)
                }else if(updateDataTmp.get("status") == athenaConst.PROCESSED_HL7) {
                    updateDataTmp.put("hl7ProcessedDate",currentDate)
                }
                DBObject updateData = new BasicDBObject(new BasicDBObject ("\$set", new BasicDBObject(updateDataTmp)));
                mongoService.collection(athenaConst.STAGING_COLLECTION).updateOne(searchCriteria, updateData)//, upsert, multiUpdate, WriteConcern.SAFE)
                resultMap.put("isSuccess", true);
            }else {
                log.error("updateStagingData : Not able to update data due to missing/misconfigured searchCriteria or updateCriteria. It should be Map. Received requestMap: "+requestMap);
                resultMap.put("isSuccess", false);
                resultMap.put("message", "missing or misconfigured search or update criteria");
            }
        }catch (Exception ex){
            resultMap.put("isSuccess",false);
            resultMap.put("message",ex.getMessage());
            log.error("updateStagingData : Not able to update data in staging collection for requestMap=>"+requestMap+" Exception:"+ex.getMessage(), ex);
        }finally{

        }
        return resultMap;
    }

    /**
     *
     * @param modField
     * @return
     */
    public Integer getJobInstanceNumber(String modField, String hl7Type = "", String apiName = ""){
        int jobInstanceNumber = 0;
        if(athenaConf?.maxJobInstanceNumber != null && modField && hl7Type == ""){
            //Write down the logic for jobInstance Number
            return (Math.abs(modField.hashCode()) % Integer.parseInt(athenaConf?.maxJobInstanceNumber));
        }else if(modField && hl7Type != "" && athenaConf?.hl7JobInstance && athenaConf?.hl7JobInstance.containsKey(hl7Type)){
            return (Math.abs(modField.hashCode()) % Integer.parseInt(athenaConf?.hl7JobInstance.get(hl7Type)));
        } else if (modField != null && apiName != null){
            return (Math.abs(modField.hashCode()) % athenaConf?.jobInstanceNumberForCDC.get(apiName));
        }
        return jobInstanceNumber;
    }

    /**
     *
     * @param requestMap
     * @param primaryId
     * @param metaList
     * @return
     */
    public List createOrUpdateMetadata(Map requestMap, String primaryId, List metaList = []){
        if (log.isDebugEnabled())
            log.debug("createOrUpdateMetadata() : requestMap received is -> ${requestMap} | primaryId - ${primaryId}")
        List metadataList = new ArrayList(metaList);
        Map enrty = new LinkedHashMap()
        enrty.put(requestMap.get("apiName"), requestMap.get("data").containsKey(primaryId) ? requestMap.get("data").get(primaryId).toString():"");
        if(enrty.get(requestMap.get("apiName")) == "") {
            enrty.put(requestMap.get("apiName"), requestMap.containsKey(primaryId) ? requestMap.get(primaryId).toString() : "");
        }
        metadataList.add(enrty);
        return metadataList;
    }

    /**
     *
     * @param apiConfig
     * @param requestMap
     * @return
     */
    public List createStagingDataList(Map apiConfig, Map requestMap){
        List resultList = new ArrayList();
        try {
            if (apiConfig.get("apiType") == athenaConst.SUBSCRIPTION || apiConfig.get("apiType") == athenaConst.SEARCH) {
                log.info("createStagingDataMap started with multiple data set.")
                requestMap.get("result").get(apiConfig.get("apiResponseKey")).each { data ->
                    Map resultMap = createDataMap(apiConfig, data, requestMap.get("additionalInfo"))
                    resultList.add(resultMap);
                }
            } else {
                log.info("createStagingDataMap started with single data set.")
                requestMap.get("result").each { data ->
                    Map resultMap = createDataMap(apiConfig, data, requestMap.get("additionalInfo"))
                    resultList.add(resultMap);
                }
            }
        } catch (Exception e) {
            log.error("createStagingDataList() : Exception occurred while creating DataList for staging data. Request map is - ${requestMap} => "+e.getMessage(), e)
        }

        return resultList;
    }
    /**
     *
     * @param apiConfig
     * @param dataMap
     * @param additionalInfo
     * @return
     */
    private Map createDataMap(Map apiConfig, Map dataMap, Map additionalInfo){
        Map resultDataMap = new LinkedHashMap();
        resultDataMap.put("data",dataMap);
        resultDataMap.put("modField",(dataMap.get(apiConfig.get("modField")))? dataMap.get(apiConfig.get("modField").toString()) : "");
        resultDataMap.put("practiceId",additionalInfo.get("practiceid"));
        //We have assume that each subscription API will contain the departmentid TODO Need to validate for All APIs
        resultDataMap.put("departmentId", (dataMap?.departmentid)?:"");
        resultDataMap.put("apiName",additionalInfo.get("apiName"));
        return resultDataMap;
    }

    public List<IdentifierMetaData> createIdentifierDTO(List requestList){
        List<IdentifierMetaData> dtoList= new ArrayList<>();
        requestList.each { data ->
            IdentifierMetaData idmd = new IdentifierMetaData();
            idmd.assigningAuthority = data.get("AA");
            idmd.idValue = data.get("id");
            idmd.typeCode = data.get("TC");
            idmd.useCode = data.get("useCode");
            dtoList.add(idmd);
        }
        return dtoList;
    }

    public List<HumanNameMetaData> createHumanNameMetaDTO(List requestList){
        List<HumanNameMetaData> dtoList= new ArrayList<>();
        requestList.each { data ->
            HumanNameMetaData hnmd = new HumanNameMetaData();
            hnmd.patientFirstName = data.get("firstname");
            hnmd.patientLastName = data.get("lastname");
            hnmd.patientMiddleName = data.get("middlename");
            hnmd.patientNameLanguage = "en";
            hnmd.patientNamePrefix = data.get("prefix");
            hnmd.patientNameSuffix = data.get("suffix");
            dtoList.add(hnmd);
        }
        return dtoList;
    }

    public List<HumanNameDTO> createHumanNameDTO(List requestList){
        List<HumanNameDTO> dtoList= new ArrayList<>();
        requestList.each { data ->
            HumanNameDTO hnmd = new HumanNameDTO();
            hnmd.firstName = data.get("firstname");
            hnmd.lastName = data.get("lastname");
            hnmd.middleName = data.get("middlename");
            hnmd.lang = "en";
            hnmd.prefix = data.get("prefix");
            hnmd.suffix = data.get("suffix");
            dtoList.add(hnmd);
        }
        return dtoList;
    }

    public List<AddressMetadata> createAddressDTO(List requestList){
        List<AddressMetadata> dtoList= new ArrayList<>();
        requestList.each { data ->
            AddressMetadata amd = new AddressMetadata();
            amd.useCode = data.get("useCode");
            amd.patientCity = data.get("city");
            amd.patientStrAdd1 = data.get("address1");
            amd.patientStrAdd2 = data.get("address2");
            amd.patientState = data.get("state");
            amd.patientZip = data.get("zip");
            amd.patientCountry = data.get("country");
            dtoList.add(amd);
        }
        return dtoList;
    }

    public List<NK1Segment> createNK1SegmentDTO(List requestList){
        List<NK1Segment> dtoList= new ArrayList<>();
        requestList.each { data ->
            NK1Segment nk1 = new NK1Segment();
            nk1.relatedPersonFirstName = data.get("name");
            nk1.relatedPersonLastName = data.get("family");
            nk1.relatedPersonRelationship = data.get("relationship");
            nk1.phone = new ArrayList<TelecommunicationNumber>();
            if(data.get("homephone")){
                TelecommunicationNumber homePh = new TelecommunicationNumber();
                homePh.typeCode = "homePhone";
                homePh.idValue = data.get("homephone");
                homePh.countryCode = data.get("countrycode");
                homePh.system = "phone"
                nk1.phone.add(homePh);
            }
            if(data.get("mobilephone")){
                TelecommunicationNumber homePh = new TelecommunicationNumber();
                homePh.typeCode = "mobilePhone";
                homePh.idValue = data.get("mobilephone");
                homePh.countryCode = data.get("countrycode");
                homePh.system = "phone"
                nk1.phone.add(homePh);
            }
            dtoList.add(nk1);
        }
        return dtoList;
    }

    public List<ContactPointMetaData> createTelecomListDTO(List requestList){
        List<ContactPointMetaData> dtoList= new ArrayList<>();
        requestList.each { data ->
            ContactPointMetaData cpmd = new ContactPointMetaData();
            cpmd.countryCode = data.get("countryCode");
            cpmd.phoneSystemValue = data.get("phoneSystemValue");
            cpmd.phoneUseCode = data.get("phoneUseCode");
            cpmd.phoneValue = data.get("phoneValue");
            dtoList.add(cpmd);
        }
        return dtoList;
    }

    public List<CodeableConceptDTO> createCodeableConceptDTOList(List requestList){
        List<CodeableConceptDTO> dtoList= new ArrayList<>();
        requestList.each { data ->
            CodeableConceptDTO ccd = new CodeableConceptDTO();
            CodingDTO cd = new CodingDTO();
            cd.code = data.get("code");
            cd.display = data.get("display");
            cd.system = data.get("system")
            ccd.coding = new ArrayList<>()
            ccd.coding.add(cd)
            ccd.text = data.get("display");
            dtoList.add(ccd);
        }
        return dtoList;
    }

    public List<HealthcareServiceSegment> createHealthcareServiceSegmentsList(List requestList){
        List<HealthcareServiceSegment> dtoList= new ArrayList<>();
        requestList.each { data ->
            HealthcareServiceSegment hcs = new HealthcareServiceSegment();
            hcs.identifier = createIdentifierDTOList([["identifier": data.get("code"), "TC":"specialtyIdentifier", "useCode":"usual"]]).getAt(0);
            hcs.serviceName = data.get("display");
            hcs.location = createLocationSegment([data]).getAt(0)

            List<CodeableConceptDTO> type = new ArrayList<>()
            CodeableConceptDTO typeCode = new CodeableConceptDTO()
            typeCode.text = StringConstants.CONSULTATION_SERVICE_LINE
            List<CodingDTO> coding = new ArrayList<>()
            CodingDTO typeCoding = new CodingDTO()
            typeCoding.code = "383"
            typeCoding.display = StringConstants.CONSULTATION_SERVICE_LINE
            coding.add(typeCoding)
            typeCode.coding = coding
            type.add(typeCode)
            hcs.type = type

            CodeableConceptDTO category = new CodeableConceptDTO()
            category.text = "Hospital"
            List<CodingDTO> coding1 = new ArrayList<>()
            CodingDTO typeCoding1 = new CodingDTO()
            typeCoding1.code = StringConstants.HOSPITAL_HCS_CATEGORY_CODE
            typeCoding1.display = "Hospital"
            coding1.add(typeCoding1)
            category.coding = coding1
            hcs.category = category

            //TODO need to support for type. Not available in Minerva V3.5.2
            //hcs.type
            dtoList.add(hcs);
        }
        return dtoList;
    }

    public List<LocationSegment> createLocationSegment(List requestList){
        List<LocationSegment> dtoList= new ArrayList<>();
        requestList.each { data ->
            LocationSegment loc = new LocationSegment();
            loc.identifier = createIdentifierDTOList([["identifier": data.get("locationId"), "TC":"performingLocationId", "useCode":"usual"]]).getAt(0);
            loc.name = data.get("locationName");
            loc.physicalType = "bu"
            dtoList.add(loc);
        }
        return dtoList;
    }

    public List<AnnotationDTO> createAnnotationDTOList(List requestList){
        List<AnnotationDTO> dtoList= new ArrayList<>();
        requestList.each { data ->
            AnnotationDTO and = new AnnotationDTO();
            and.text = data.get("text")
            if(data?.get("time")){
                and.time = data?.get("time")
            }
            and.value = [data?.get("text")?.toString()]
            dtoList.add(and);
        }
        return dtoList;
    }

    public List<IdentifierDTO> createIdentifierDTOList(List requestList){
        List<IdentifierDTO> dtoList= new ArrayList<>();
        requestList.each { data ->
            IdentifierDTO cs = new IdentifierDTO();
            cs.identifier = data.get("identifier");
            cs.typeCode = data.get("TC");
            cs.assigningAuthority = data.get("AA");
            cs.useCode = data.get("useCode");
            dtoList.add(cs);
        }
        return dtoList;
    }

    public List<PractitionerRoleDTOV2> createPractitionerRoleDTOList(List requestList){
        List<PractitionerRoleDTOV2> dtoList= new ArrayList<>();
        requestList.each { data ->
            PractitionerRoleDTOV2 prd = new PractitionerRoleDTOV2();
            prd.roleCode = "MD"  //TODO need to fetch details from provider API key "providertype"
            prd.roleDisplay = "PHYSICIAN" //TODO need to correct the mapping at later stage
            prd.specialityCode = data.get("specialtyid")
            prd.specialityDisplay = data.get("specialty")
            prd.specialityText = data.get("specialty")
            dtoList.add(prd);
        }
        return dtoList;
    }

    public List<PractitionerMetaDataV2> createPractitionerDTOList(List requestList){
        List<PractitionerMetaDataV2> dtoList= new ArrayList<>();
        requestList.each {Map data ->
            PractitionerMetaDataV2 pmd = new PractitionerMetaDataV2();
            pmd.identifierDTO = genrateIdentifierDTOList(data?.identifier)//createIdentifierDTOList([data]);
            pmd.practitionerType = data.get("practitionerType");
            pmd.humanNameDTO = createHumanNameDTO([data]);
            pmd.gender = data.get("gender");
            if(data.get("specialty") && data.get("specialtyid")){
                pmd.practitionerRoles = createPractitionerRoleDTOList([data]);
            }
            //List practitionerTelecomList = getTelecomList(data.get("telecomList"));
            if(data.npi) {
                pmd.npiIdentifier = createIdentifierDTOs("npi", data?.npi?.toString(), "NPI")?.getAt(0)
            }
            pmd.telecomList = createTelecomDTO(data.get("telecomList"));
            List practitionerAddressList = getAdressList(data.get("addressList"));
            pmd.addresses = createAddressDTOList(practitionerAddressList);
            dtoList.add(pmd);
        }
        return dtoList;
    }

    public List<IdentifierDTO> genrateIdentifierDTOList(List identifierList){
        List<IdentifierDTO> identifierDTOs = new LinkedList<>();
        for(def identifier : identifierList){
            IdentifierDTO identifierDTO = new IdentifierDTO(identifier?.type?.coding?.getAt(0)?.code,identifier?.value,identifier?.system)
            identifierDTOs.add(identifierDTO)
        }
        return identifierDTOs
    }

    public List<Map> getAdressList(List<Map> addressList){
        List resourceAddressList = new ArrayList();
        for (Map addressMap :  addressList) {
            Map resourceAddressMap = new LinkedHashMap();
            resourceAddressMap.put("address1", addressMap?.get("line")?.getAt(0));
            resourceAddressMap.put("address2", (addressMap?.get("line")?.size() > 1) ? addressMap?.get("line")?.get(1): "");
            resourceAddressMap.put("city", addressMap?.get("city"));
            resourceAddressMap.put("state", addressMap?.get("state"));
            resourceAddressMap.put("zip", addressMap?.get("postalCode"));
            resourceAddressMap.put("country", addressMap?.get("country"));
            resourceAddressMap.put("useCode", addressMap?.get("use"));
            resourceAddressList.add(resourceAddressMap);
        }
        return resourceAddressList;
    }

    public List<Map> getTelecomList(List<Map> telecomList){
        List resourceTelecomList = new ArrayList();
        for (Map telecomMap : telecomList){
            Map resourceTelecomMap = new LinkedHashMap();
            if (telecomMap.system.equals("email")){
                resourceTelecomMap.put("phoneSystemValue", "email")
                resourceTelecomMap.put("phoneUseCode", "email")
                resourceTelecomMap.put("phoneValue", telecomMap.get("value"));
            } else {
                resourceTelecomMap.put("countryCode", athenaConf.countryCode)
                resourceTelecomMap.put("phoneSystemValue", "phone")
                resourceTelecomMap.put("phoneUseCode", telecomMap.get("use"))
                resourceTelecomMap.put("phoneValue", telecomMap.get("value"))
            }
            resourceTelecomList.add(resourceTelecomMap);
        }
        return resourceTelecomList
    }

    public Date dateAddOperation(Date date,int hour, int minute,int second){
        Date customDate = null
        try{
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.HOUR, hour);
            cal.add(Calendar.MINUTE, minute);
            cal.add(Calendar.SECOND, second);
            customDate = cal.getTime()
        }
        catch(Exception ex){
            log.error("Exception in dateHourMinSecCustom : ",ex)
        }
        return customDate
    }
    public String strToStrFormatTimeZoneConvert(String inputDate,String inputFormat , String outputFormat, String timeZone = null) {
        /*
        timeZone will be like this "GMT+8"
         */
        if (log.isDebugEnabled())
            log.debug("Inside strToStrFormatTimeZoneConvert with inputDate : ${inputDate} , inputFormat : ${inputFormat} , outputFormat : ${outputFormat} , timeZone (NON_mandatory) : ${timeZone}")
        String convertedDate = null
        try{
            if(inputDate && inputFormat && outputFormat){
                DateFormat outputDateFormat = new SimpleDateFormat(outputFormat);
                DateFormat inputDateFormat = new SimpleDateFormat(inputFormat);
                Date gmt_time = inputDateFormat.parse(inputDate);
                if(timeZone){
                    TimeZone gmtTime = TimeZone.getTimeZone(timeZone);
                    outputDateFormat.setTimeZone(gmtTime);
                }
                convertedDate = outputDateFormat.format(gmt_time)
            }
            else{
                log.error("Inside strToStrFormatTimeZoneConvert : Invalid inputDate : ${inputDate} , inputFormat : ${inputFormat} , outputFormat : ${outputFormat}")
            }
        }
        catch(Exception ex){
            log.error("Exception in strToStrFormatTimeZoneConvert : ",ex)
        }
        return convertedDate
    }

    public String dateToStrFormatTimeZoneConvert(Date inputDateTime, String timeFormat,String timeZone = null){
        /*
        timeZone will be like this "GMT+8"
         */
        if (log.isDebugEnabled())
            log.debug("Inside dateToStrFormatTimeZoneConvert inputDateTime : ${inputDateTime} , timeFormat : ${timeFormat} , timeZone (NON_mandatory)  : ${timeZone}")
        String convertedDate = null
        try{
            if(inputDateTime && timeFormat){
                DateFormat dateFormat = new SimpleDateFormat(timeFormat);
                if(timeZone){
                    TimeZone timeZoneObject = TimeZone.getTimeZone(timeZone);
                    dateFormat.setTimeZone(timeZoneObject);
                }
                convertedDate = dateFormat.format(inputDateTime)
            }
            else{
                log.error("Inside dateToStrFormatTimeZoneConvert : Invalid inputDateTime : ${inputDateTime} , timeFormat : ${timeFormat}")
            }
        }
        catch(Exception ex){
            log.error("Exception in dateToStrFormatTimeZoneConvert : ",ex)
        }
        return convertedDate;
    }

    public Date strToDateFormatTimeZoneConvert(String inputDate,String inputFormat = null, String timeZone = null) {
        log.debug("Inside strToDateFormatTimeZoneConvert with inputDate : ${inputDate} , inputFormat : ${inputFormat} ,  timeZone (NON_mandatory) : ${timeZone}")
        Date convertedDate = null
        try{
            if(inputDate && inputFormat){
                DateFormat inputDateFormat = new SimpleDateFormat(inputFormat);
                if(timeZone){
                    TimeZone gmtTime = TimeZone.getTimeZone(timeZone);
                    inputDateFormat.setTimeZone(gmtTime);
                }
                convertedDate = inputDateFormat.parse(inputDate);
            }
            else{
                if(inputDate){
                    convertedDate = DateUtils.parsedISO8601Date(inputDate,timeZone,true)
                }else {
                    log.debug("Inside strToDateFormatTimeZoneConvert : Invalid inputDate : ${inputDate} , inputFormat : ${inputFormat}")
                }
            }
        }
        catch(Exception ex){
            log.error("Exception in strToDateFormatTimeZoneConvert : ",ex)
        }
        return convertedDate
    }

    public List<TelecomDTO> createTelecomDTO(List requestList){
        List<TelecomDTO> dtoList= new ArrayList<>();
        requestList.each { data ->
            TelecomDTO td = new TelecomDTO();
            td.countryCode = data.get("countryCode")?: athenaConf.countryCode;
            td.value = data.get("value");
            td.system = data.get("system");
            td.useCode = data.get("useCode");
            dtoList.add(td);
        }
        return dtoList;
    }

    public List<AddressDTO> createAddressDTOList(List requestList){
        List<AddressDTO> dtoList= new ArrayList<>();
        requestList.each { data ->
            AddressDTO adto = new AddressDTO();
            adto.line1 = data.get("address1");
            adto.line2 = data.get("address2");
            adto.city = data.get("city");
            adto.state = data.get("state");
            adto.zip = data.get("zip");
            adto.country = data.get("country");
            adto.code = data.get("useCode")
            dtoList.add(adto);
        }
        return dtoList;
    }

    public List<DBObject> getAthenaMaster(String practiceId, Map apiConfig){
//        def responseObject = mongoService.search(new BasicDBObject("masterJson.practiceid",practiceId).append("masterType" , "PRACTICEINFO"),"athenaMaster")
//        ObjectId practiceMongoId = responseObject?.objects?.size()>0 ?responseObject?.objects?.getAt(0)?._id : null
        List<DBObject> objects = new ArrayList<DBObject>(); //@TODO to remove object
        BasicDBList andQuery = new BasicDBList();
        andQuery.add(new BasicDBObject("practiceId", new BasicDBObject('$eq', practiceId)))
        andQuery.add(new BasicDBObject('masterType', new BasicDBObject('$eq':apiConfig.masterType)))
        andQuery.add(new BasicDBObject('status', new BasicDBObject('$eq':athenaConst.ACTIVE)))
        BasicDBObject query = new BasicDBObject('$and', andQuery)
        log.info("getAthenaDepartments >> Query object created is - ${query}")
        Map responseMap = mongoService.search(query, "athenaMaster");
        log.info(responseMap)
        if(responseMap?.objects && responseMap?.objects?.size() > 0) {
            objects = responseMap?.objects
        }
        return objects
    }

    public boolean createOrUpdateMasterRecords(String practiceId, Map athenaDocumentMap, Map apiConfig) {
        boolean mongoSuccessFlag = false;
        List searchQueryList = [];
        BasicDBObject updateObject;
        BasicDBObject searchQuery;
        log.info("createOrUpdateMasterRecords: Going to Create Or Update master records.")
        for (entry in athenaDocumentMap.entrySet()) {
            searchQueryList = [];
            if(apiConfig.masterType.equals("DEPARTMENT")) {
                searchQueryList.add(new BasicDBObject("practiceId", new BasicDBObject("\$eq", practiceId)));
            }
            searchQueryList.add(new BasicDBObject("masterType", new BasicDBObject("\$eq", apiConfig.masterType)));
            searchQueryList.add(new BasicDBObject("masterId", new BasicDBObject("\$eq", entry.getKey())));
            searchQuery = new BasicDBObject("\$and",searchQueryList);
            updateObject = new BasicDBObject();
            updateObject.append("\$set", new BasicDBObject().append("masterType", apiConfig.masterType).append("practiceId",practiceId).append("dateCreated", new Date()).append("masterId", entry.getKey()).append("status",athenaConst.ACTIVE).append("lastUpdated", new Date()).append("masterJson", entry.getValue()));
            mongoSuccessFlag = mongoService.update(searchQuery, updateObject, "athenaMaster", true);
            //TODO: Flag being a boolean, does not necessarily needs a true comparison check guys and where are the negative check logs?
            if (mongoSuccessFlag){
                log.info("AthenaMasterIngestionService: Going to Create Location Objecy for Practice '${practiceId}'")
                if (createOrUpdateLocation(practiceId, entry.value, apiConfig)){
                    log.info("AthenaMasterIngestionService: Location Successful created for Practice '${practiceId}'");
                } else {
                    log.info("AthenaMasterIngestionService: Location creation failed for Practice '${practiceId}'");
                }
            }

        }

        if (athenaDocumentMap?.keySet()?.size() > 0) {
            log.info("createOrUpdateMasterRecords: Going to mark records that were not found in the api call as Inactive.")
            searchQueryList = [];
            searchQueryList.add(new BasicDBObject("practiceId", new BasicDBObject("\$eq", practiceId)));
            searchQueryList.add(new BasicDBObject("masterType", new BasicDBObject("\$eq", apiConfig.masterType)));
            searchQueryList.add(new BasicDBObject("masterId", new BasicDBObject("\$nin", athenaDocumentMap.keySet())));
            searchQuery = new BasicDBObject("\$and",searchQueryList);
            updateObject = new BasicDBObject()
            updateObject.append("\$set", new BasicDBObject().append("lastUpdated", new Date()).append("status",athenaConst.INACTIVE));
            mongoSuccessFlag = mongoService.update(searchQuery, updateObject, "athenaMaster", false, true);
        }

        return mongoSuccessFlag;
    }

    private boolean createOrUpdateLocation(String practiceId, def locationDocument, Map apiConfig ) {
        boolean isSuccess = false

        try {
            Map searchMap = [:]
            if (apiConfig.masterType.equals("DEPARTMENT")) {
                searchMap.put("identifier", practiceId + "_" + locationDocument.departmentid); //@TODO to discuss on identifer for DEPARTMENT @Himanshu , @Naren
            } else {
                searchMap.put("identifier", practiceId);
            }
            List<LocationResource> locationResources = new LocationResource().search(searchMap).list;
            LocationResource locationResource = new LocationResource();
            boolean isSave = true
            if (locationResources?.size() == 1 ){
                locationResource = locationResources.getAt(0)
                isSave = false
            } else if (locationResources?.size() > 1 ){
                throw new Exception("Duplicate Location entries found :" + searchMap.toString())
            }

            locationResource.name = new StringType(locationDocument?.name);
            List<Identifier> identifierList = new LinkedList<>();
            String identifierValue;
            if(apiConfig.masterType.equals("DEPARTMENT")) {
                identifierValue = new StringType(practiceId+"_"+locationDocument.departmentid)
                Map parentSearchMap = ["identifier": practiceId]
                List<LocationResource> parentLocationResourceList = new LocationResource().search(parentSearchMap).list;
                if(parentLocationResourceList?.size() > 0){
                    locationResource.partOf = parentLocationResourceList.getAt(0);
                }
            } else {
                identifierValue = new StringType(practiceId)
            }
            Identifier identifier = Identifier.createIdentifier("performingLocationId","usual",identifierValue,"Athena")
            identifierList.add(identifier)
            locationResource.identifier = identifierList;

            List<ContactPoint> contactPointList = new LinkedList<>();

            if (locationDocument?.phone){
                ContactPoint contactPointPhone = new ContactPoint("phone","phone",locationDocument?.phone?.toString(),athenaConf?.countryCode?.toString());
                contactPointList.add(contactPointPhone)
            }

            if (locationDocument?.fax){
                ContactPoint contactPointFax = new ContactPoint("phone","fax",locationDocument?.fax?.toString(),athenaConf?.countryCode?.toString());
                contactPointList.add(contactPointFax)
            }

            locationResource.telecom = contactPointList

            if (locationDocument?.address || locationDocument?.state || locationDocument?.city || locationDocument?.zip) {
                List<Address> addressList = new LinkedList<>();
                Address address = new Address(locationDocument?.address,locationDocument?.address2,locationDocument?.city,locationDocument?.state,locationDocument?.zip,locationDocument?.country,locationDocument?.district,"work");
                addressList.add(address)
                locationResource.address = addressList

            }

            if(isSave){
                isSuccess = locationResource.save();
            }else {
                isSuccess = locationResource.update();
            }
        } catch ( Exception ex){
            log.error("Exception createOrUpdateLocation:" , ex) //@TODO Correct log message with practice and department ID
        }
        return isSuccess;
    }

    public boolean checkJobConfigStatus(Long jobConfigId) {
        BasicDBObject jobConfigSearchQuery = new BasicDBObject("_id",jobConfigId)
        Map jobConfigSearchResult = mongoService.search(jobConfigSearchQuery, "jobConfiguration")
        log.info("checkJobConfigStatus(): Job config current status -> "+jobConfigSearchResult)
        return true
    }

    public boolean checkForMemberData(String assigningAthority,String patientId){
        return mongoService.search(new BasicDBObject("identifier", new BasicDBObject("\$elemMatch", new BasicDBObject("system.value": assigningAthority).append("value.value", patientId))).append("identifier.system.value", "AIM"), "patient")?.totalCount?.count > 0
    }

    public Map saveClinicalOrReferenceData(Map requestMap,Map resultMap){
        if (log.isDebugEnabled())
            log.debug("saveClinicalData() : requestMap received is -> ${requestMap} and ${resultMap}")
        Map responseMap = [:];
        try{
            //Creating the document structure going to be saved
            Date currentDate = new Date();
            DBObject athenaDocument = new BasicDBObject();
            athenaDocument.put("jobInstanceNumber",requestMap.get("jobInstanceNumber"));
            athenaDocument.put("retries",0);
            athenaDocument.put("status",athenaConst.INITIATED);
            athenaDocument.put("dateCreated",currentDate);
            athenaDocument.put("lastUpdated",currentDate);
            athenaDocument.put("practiceId",requestMap.get("practiceId"));
            athenaDocument.put("stagingId",requestMap.get("stagingId"));
            athenaDocument.put("apiUrl", resultMap.get("apiUrl"));
            athenaDocument.put("sourceOfOrigin",requestMap.get("source"));
            if (!requestMap.get("apiName").equals("patientReference")){
                athenaDocument.put("departmentId",(requestMap.get("departmentId")));
            }
            athenaDocument.put("apiName",requestMap.get("apiKey")? requestMap.get("apiKey") : requestMap.get("apiName"));
            athenaDocument.put("patientId",(requestMap.get("patientId"))?:"");
            if(resultMap.entry){
                for(def entry :resultMap.entry) {
                    if (!entry?.resource?.issue?.find{it?.severity?.equals("error")}) {
                        if((requestMap.get("apiKey").equals("conditionProblemClinicalReference") && entry?.resource?.category?.coding?.code == "encounter-diagnosis") || (requestMap.get("apiKey").equals("conditionDiagnosisClinicalReference") && entry?.resource?.category?.coding?.code == "problem-list-item")){
                            continue;
                        }
                        // Adding Hl7Creation status map on the basis of apiName Config bucket
                        if (requestMap.get("apiKey") == null && requestMap.get("apiName").equals("patientReference")) {
                            if(requestMap.get("isAppointmentSubscription")){
                                if(requestMap.get("isMemberData")) {
                                    athenaDocument.put("hl7CreationStatusMap", athenaConf.patientReferenceSetForAppSubs)
                                    athenaDocument.put("isMemberData", true)
                                }else{
                                    athenaDocument.put("hl7CreationStatusMap", athenaConf.patientReferenceSetForNonMember)
                                    athenaDocument.put("isMemberData", false)
                                }
                            }else {
                                athenaDocument.put("hl7CreationStatusMap", athenaConf.patientReferenceSet)
                                athenaDocument.put("isMemberData", true)
                            }
                        } else if (requestMap.get("apiKey") == null && requestMap.get("apiName").equals("encounterReference")) {
                            athenaDocument.put("hl7CreationStatusMap", athenaConf.encounterReferenceSet)
                            athenaDocument.put("isMemberData", true)
                        }

                        if (requestMap.get("apiKey").equals("observationClinicalReference")){
                            athenaDocument.put("encounterId",requestMap.get("encounterId"))
                        }

                        // Method to add encounterID in resource ---
                        String encounterId = fetchEncounterId(entry, requestMap);

                        if (encounterId != null && encounterId != "") {
                            athenaDocument.put("encounterId", encounterId)
                        }

                        if (requestMap.get("apiName").equals("encounterReference") && entry?.resource?.location) {
                            for (def locationMap : entry?.resource?.location) {
                            athenaDocument.put("departmentId", locationMap?.location?.reference?.split("-")?.last());
                            }
                    }

                    athenaDocument.resource = entry.resource;
                    if(entry?.resource?.resourceType?.equals("Practitioner")){
                        if(mongoService.search(new BasicDBObject("practiceId",requestMap.get("practiceId")).append("resource.id",entry?.resource?.id).append("apiName","practitionerReference"),athenaConst?.REFERENCE_COLLECTION)?.totalCount?.count == 0) {
                            athenaDocument.put("apiName", "practitionerReference");
                            athenaDocument.remove("departmentId")
                            athenaDocument.remove("patientId")
                            athenaDocument.remove("stagingId")
                        }else{
                            log.debug("Practitioner data already present in database.")
                            continue;
                        }
                    }
                    //Going to insert the document
                    Boolean isSuccess = true
                        if (["immunizationClinicalReference", "clinicalImpressionClinicalReference", "medicationAdministrationClinicalReference", "diagnosticReportClinicalReference"].contains(requestMap.get("apiKey")) || (requestMap.get("apiKey") == null && requestMap.get("apiName") == "encounterReference")) {
                        isSuccess = createPractitionerReference(requestMap,entry?.resource,requestMap.get("apiKey"))
                    }
                    if(["medicationRequestClinicalReference","medicationAdministrationClinicalReference"].contains(requestMap.get("apiKey"))){
                        isSuccess = createMedicationReference(requestMap, entry?.resource,requestMap.get("apiKey"))
                    }
                    if(["diagnosticReportClinicalReference"].contains(requestMap.get("apiKey")) && entry?.resource?.result && entry?.resource?.result?.size() > 0){
                        isSuccess = createReportObservationReference(requestMap, entry?.resource,requestMap.get("apiKey"))
                    }


                    if(isSuccess) {
                        isSuccess = mongoService.save(athenaDocument.toJson(), athenaConst.REFERENCE_COLLECTION)
                    }
                    responseMap.put("isSuccess", isSuccess);
                    } else {
                        log.debug("Received Error response :"+entry?.resource?.issue?.find{it?.severity?.equals("error")}?.details?.text)
                    }
                }
            }else{
                responseMap.put("isSuccess", true)
            }

        }catch (Exception ex){
            responseMap.put("isSuccess",false);
            responseMap.put("message",ex.getMessage());
            log.error("saveClinicalData : Not able to ingest data in reference collection. Request map is - ${requestMap}, Result map is - ${resultMap} =>"+ex.getMessage(), ex);
        }finally{

        }
        return responseMap;
    }


    private String fetchEncounterId(def entry, Map requestMap){
        String encounterId

        if ( requestMap.get("apiKey") == null && requestMap.get("apiName").equals("encounterReference")){
            encounterId = entry?.resource?.id?.split("-")?.last()
        }
        else if (athenaConf.apiKeysForPopulatingEncounterId.contains(requestMap.get("apiKey"))){
            if (requestMap.get("apiKey").equals("documentReferenceClinicalReference")){
                encounterId = entry?.resource?.context?.encounter?.find{it?.reference?.contains("Encounter")}?.reference?.split("-")?.last()
            }
            else if (requestMap.get("apiKey").equals("medicationAdministrationClinicalReference")){
                encounterId = entry?.resource?.encounter?.reference?.split("/")?.last()
            }
            else if (requestMap.get("apiKey").equals(athenaConst?.CLINICALIMPRESSION_CLINICALREFERENCE)){
                encounterId = getDateBasedEncounterId(entry?.resource?.date,requestMap?.patientId,requestMap?.practiceId)
            }
            else{
                encounterId = entry?.resource?.encounter?.reference?.split("-")?.last()
            }

        }

        return encounterId;
    }



    public Boolean createPractitionerReference(Map requestMap,Map resourceMap,String apiName){
        Boolean isSuccess = true
        String[] pracIdList = null;
        List practitionerIdList = [] ;
        if(apiName.equals("immunizationClinicalReference")) {
            pracIdList = resourceMap?.performer?.size() > 0 ? resourceMap?.performer?.getAt(0)?.actor?.reference?.split("/") : null;
        }
        if(apiName.equals("clinicalImpressionClinicalReference")){
            pracIdList = resourceMap?.assessor?.reference?.split("/");
        }
        if(apiName.equals("medicationAdministrationClinicalReference")){
            pracIdList = resourceMap?.practitioner?.reference?.split("/");
        }
        if(apiName.equals("diagnosticReportClinicalReference") && resourceMap?.performer){
            pracIdList = resourceMap?.performer?.getAt(0)?.reference?.split("/");
        }
        if(apiName.equals("providerReference")){
            practitionerIdList.add("a-"+requestMap.practiceId+".Provider-"+resourceMap?.provider)
        }
        if(pracIdList?.size() == 2 && pracIdList[0].equals("Practitioner")){
            if(apiName.equals("immunizationClinicalReference") || apiName.equals("diagnosticReportClinicalReference"))
                practitionerIdList.add(pracIdList[1])
            else
                practitionerIdList.add("a-"+requestMap.practiceId+".Provider-"+pracIdList[1])
        }
        if(apiName.equals("encounterReference")){
            for(Map entry : resourceMap?.participant){
                if(entry?.individual?.reference?.split("/")?.size() == 2 && entry?.individual?.reference?.split("/")[0].equals("Practitioner")){
                    practitionerIdList.add(entry?.individual?.reference?.split("/")[1])
                }
            }
        }

        for(int i=0;i<practitionerIdList.size();i++) {
            String practitionerId = practitionerIdList[i]
            BasicDBObject practitionerSearchCriteria = new BasicDBObject("practiceId", requestMap?.practiceId).append("resource.id", practitionerId).append("apiName", "practitionerReference");
            Map practitionerMap = mongoService.search(practitionerSearchCriteria, athenaConst.REFERENCE_COLLECTION)
            if (practitionerMap.totalCount.count > 0) {
                isSuccess = true
                log.info("(createPractitionerReference) => Practitioner data for Id : ${practitionerId} already exists for stagingId : ${requestMap?.stagingId}  | practiceId : ${requestMap?.practiceId}")
            }else{
                Map pracApiConfig = athenaConst.getAPIConfig("providerReference");
                Map reqMap = ["ah-practice" : "Organization/a-" + requestMap.departmentId + ".Practice-" + requestMap.practiceId,
                              "providerid" : practitionerId
                ];
                def resultMap = athenaCallAPI(pracApiConfig, reqMap).result;
                if(resultMap){
                    Date currentDate = new Date();
                    DBObject athenaDocument = new BasicDBObject();
                    athenaDocument.put("jobInstanceNumber",requestMap.get("jobInstanceNumber"));
                    athenaDocument.put("retries",0);
                    athenaDocument.put("status",athenaConst.INITIATED);
                    athenaDocument.put("dateCreated",currentDate);
                    athenaDocument.put("lastUpdated",currentDate);
                    athenaDocument.put("practiceId",requestMap.get("practiceId"));
                    athenaDocument.put("apiName","practitionerReference");
                    if(resultMap.entry){
                        for(def entry :resultMap.entry) {
                            athenaDocument.resource = entry.resource;

                            //Going to insert the document
                            isSuccess = mongoService.save(athenaDocument.toJson(), athenaConst.REFERENCE_COLLECTION)
                        }
                    }else{
                        isSuccess = true
                    }
                }
            }
        }
        return isSuccess
    }

    public Boolean createMedicationReference(Map requestMap,Map resourceMap,String apiName){
        Boolean isSuccess = false
        String[] medicationIdList = null;
        medicationIdList = resourceMap?.medicationReference?.reference?.split("/")
        String medicationId ;
        if(medicationIdList?.size() == 2 && medicationIdList[0].equals("Medication")){
            if(apiName.equals("medicationRequestClinicalReference"))
                medicationId = medicationIdList[1]
            else
                medicationId = "a-"+requestMap.practiceId+".medication-"+medicationIdList[1]
        }
        if(medicationId) {
            BasicDBObject medicationSearchCriteria = new BasicDBObject("practiceId",requestMap?.practiceId).append("resource.id",medicationId).append("apiName","medicationReference");
            Map medicationMap =  mongoService.search(medicationSearchCriteria, athenaConst.REFERENCE_COLLECTION)
            if(medicationMap.totalCount.count>0){
                isSuccess = true
                log.info("(createMedicationReference) => Medication datd for Id : ${medicationId} already exists for stagingId : ${requestMap?.stagingId}  | practiceId : ${requestMap?.practiceId}");
            }else{
                Map medicationApiConfig = athenaConst.getAPIConfig("medicationReference");
                Map reqMap = ["ah-practice" : "Organization/a-" + requestMap.departmentId + ".Practice-" + requestMap.practiceId,
                          "medicationId": medicationId
                ];

                def resultMap = athenaCallAPI(medicationApiConfig, reqMap).result;
                if(resultMap){
                    Date currentDate = new Date();
                    DBObject athenaDocument = new BasicDBObject();
                    athenaDocument.put("jobInstanceNumber",requestMap.get("jobInstanceNumber"));
                    athenaDocument.put("retries",0);
                    athenaDocument.put("status",athenaConst.INITIATED);
                    athenaDocument.put("dateCreated",currentDate);
                    athenaDocument.put("lastUpdated",currentDate);
                    athenaDocument.put("practiceId",requestMap.get("practiceId"));
                    athenaDocument.put("apiName","medicationReference");
                    if(resultMap.entry){
                        for(def entry :resultMap.entry) {
                            athenaDocument.resource = entry.resource;

                            //Going to insert the document
                            isSuccess = mongoService.save(athenaDocument.toJson(), athenaConst.REFERENCE_COLLECTION)
                        }
                    }else{
                        isSuccess = true
                    }
                }
            }
        }else{
            isSuccess = true
        }
        return isSuccess
    }

    public Boolean createReportObservationReference(Map requestMap,Map resourceMap,String apiName){
        Boolean isSuccess = false
        log.info("Inside createReportObservationReference , resourceMap : "+resourceMap)
        for(def resultReference in resourceMap?.result){
            String[] observationReferenceIdList = resultReference?.reference?.split("/")
            String observationReferenceId = null
            if(observationReferenceIdList?.size() == 2 && observationReferenceIdList[0].equals("Observation")){
                observationReferenceId = observationReferenceIdList[1]
            }
            if(observationReferenceId) {
                BasicDBObject observationSearchCriteria = new BasicDBObject("stagingId",requestMap?.stagingId).append("patientId",requestMap?.patientId).append("practiceId",requestMap?.practiceId).append("resource.id",observationReferenceId).append("apiName","observationReportClinicalReference");
                Map observationMap =  mongoService.search(observationSearchCriteria, athenaConst.REFERENCE_COLLECTION)
                if(observationMap.totalCount.count>0){
                    isSuccess = true
                    log.info("(createReportObservationReference) => Observation data for Id : ${observationReferenceId} already exists for stagingId : ${requestMap?.stagingId}  | practiceId : ${requestMap?.practiceId}");
                }else{
                    Map observationApiConfig = athenaConst.getAPIConfig("observationReportClinicalReference");
                    Map reqMap = ["ah-practice" : "Organization/a-" + requestMap.departmentId + ".Practice-" + requestMap.practiceId,
                                  "observationId" : observationReferenceId];
                    def resultMap = athenaCallAPI(observationApiConfig, reqMap).result;
                    if(resultMap){
                        Date currentDate = new Date();
                        DBObject athenaDocument = new BasicDBObject();
                        athenaDocument.put("jobInstanceNumber",requestMap.get("jobInstanceNumber"));
                        athenaDocument.put("retries",0);
                        athenaDocument.put("status",athenaConst.INITIATED);
                        athenaDocument.put("dateCreated",currentDate);
                        athenaDocument.put("lastUpdated",currentDate);
                        athenaDocument.put("practiceId",requestMap.get("practiceId"));
                        athenaDocument.put("stagingId",requestMap.get("stagingId"));
                        athenaDocument.put("encounterId",requestMap.get("encounterId"));
                        athenaDocument.put("departmentId",(requestMap.get("departmentId"))?:"");
                        athenaDocument.put("patientId",(requestMap.get("patientId"))?:"");
                        athenaDocument.put("apiName","observationReportClinicalReference");
                        if(resultMap.entry){
                            for(def entry :resultMap.entry) {
                                athenaDocument.resource = entry.resource;

                                //Going to insert the document
                                isSuccess = mongoService.save(athenaDocument.toJson(), athenaConst.REFERENCE_COLLECTION)
                            }
                        }else{
                            isSuccess = true
                        }
                    }
                }
            }else{
                isSuccess = true
            }
        }

        return isSuccess
    }

    public boolean updateAthenaCollection(BasicDBObject query, def id, String collectionName ){
        if (log.isDebugEnabled())
            log.debug("updateAthenaCollection()=> Received updateQuery "+query+" for id: "+id+" | collection "+collectionName);
        BasicDBObject updateQuery = new BasicDBObject("\$set", query)
        BasicDBObject searchQuery = new BasicDBObject("_id", id instanceof List ? new BasicDBObject("\$in", id) : id)
        mongoService.update(searchQuery, updateQuery, collectionName,false, true)
    }

    public CodeableConceptDTO getCodeableConceptDTO(String code, String display, String system, String language){
        Coding coding = new Coding(code,display,system,language);
        CodeableConcept codeableConcept = new CodeableConcept(new StringType(display),coding);
        CodeableConceptDTO codeableConceptDTO = new CodeableConceptDTO(codeableConcept);
        return codeableConceptDTO;
    }

    public def getCodeableConceptDTO(def codeableConceptData ) {
        if (codeableConceptData) {
            if (codeableConceptData instanceof List) {
                List<CodeableConceptDTO> codeableConceptDTOList = new LinkedList<>();
                for(def codeabledata : codeableConceptData){
                    CodeableConcept codeableConcept = new CodeableConcept()
                    List<Coding> codingList = new LinkedList<>();
                    String text;
                    for (def coding : codeabledata?.coding) {
                        Coding codingdata = new Coding(coding?.system, coding?.code, new StringType(coding?.display));
                        if(coding?.code?.equals("health-concern")){
                            coding?.code = "problem-list-item";
                        }
                        if(coding?.code?.equals("encounter-diagnosis") || coding?.code?.equals("problem-list-item")){
                            codingdata = new Coding(coding?.system, coding?.code, new StringType(coding?.code));
                            text = coding?.code;
                        }
                        codingList.add(codingdata);
                    }
                    codeableConcept.setCoding(codingList);
                    if(codeabledata?.text?.toString()?.toLowerCase()?.equals("active")){
                        codeableConcept.setText(new StringType(codeabledata?.text?.toString()?.toLowerCase()))
                    }else if(text){
                        codeableConcept.setText(new StringType(text));
                    }
                    else{
                    codeableConcept.setText(new StringType(codeabledata?.text?.toString()))
                    }
                    CodeableConceptDTO codeableConceptDTO = new CodeableConceptDTO(codeableConcept);
                    codeableConceptDTOList.add(codeableConceptDTO)
                }
                return codeableConceptDTOList
            } else {
                CodeableConcept codeableConcept = new CodeableConcept()
                List<Coding> codingList = new LinkedList<>();
                String text;
                for (def coding : codeableConceptData?.coding) {
                    Coding codingdata = new Coding(coding?.system, coding?.code, new StringType(coding?.display));
                    if(coding?.code?.equals("health-concern")){
                        coding?.code = "problem-list-item";
                    }
                    if(coding?.code?.equals("encounter-diagnosis") || coding?.code?.equals("problem-list-item")){
                        codingdata = new Coding(coding?.system, coding?.code, new StringType(coding?.code));
                        text = coding?.code;
                    }
                    codingList.add(codingdata);
                }
                codeableConcept.setCoding(codingList);

                if(codeableConceptData?.text?.toString()?.toLowerCase()?.equals("active")){
                    codeableConcept.setText(new StringType(codeableConceptData?.text?.toString()?.toLowerCase()))
                }else if(text){
                    codeableConcept.setText(new StringType(text));
                }
                else{
                codeableConcept.setText(new StringType(codeableConceptData?.text?.toString()))
                }


                CodeableConceptDTO codeableConceptDTO = new CodeableConceptDTO(codeableConcept);
                return codeableConceptDTO;
            }
        } else {
            return null
        }
    }

    public Quantity createQuantity(Object requestObject) {
        Quantity quantity = new Quantity();
        quantity.unit = requestObject.get("unit")
        quantity.value = requestObject.get("value")

        return quantity
    }

    public CodeableConceptDTO createCodeableConceptDTO(Object requestObject){
        CodeableConceptDTO ccd = new CodeableConceptDTO();
        ccd.coding = new ArrayList<>()
        for( def coding : requestObject.get("coding")){
            CodingDTO cd = new CodingDTO();
            cd.code = coding.get("code");
            cd.display = coding.get("display");
            cd.system = coding.get("system")
            ccd.coding.add(cd)
        }
        ccd.text = requestObject.get("text");
        return ccd;
    }

    /* commenting this out because currently we don't have support for ReferenceDTO
    private ReferenceDTO createReferenceDTO(Object requestObject){
        ReferenceDTO referenceDTO = new ReferenceDTO()
        referenceDTO.reference = requestObject.get("reference")
    } */

    public List<IdentifierDTO> createIdentifierDTOs(List identifierList, String idKey, String useCode){
        List<IdentifierDTO> identifierDTOList = []
        for (Map identifierDTO : identifierList) {
            IdentifierDTO identifier
            if(identifierDTO.system){
                identifier = new IdentifierDTO(idKey, identifierDTO?.value?.split("-").last(), identifierDTO.system)
            } else {
                identifier = new IdentifierDTO(idKey, identifierDTO?.value?.split("-").last())
            }
            identifier.useCode = useCode
            identifierDTOList.add(identifier)
        }
        return identifierDTOList
    }

    public List<IdentifierDTO> createIdentifierDTOs(String code, String value, String system){
        List<IdentifierDTO> identifierDTOList = []
        IdentifierDTO identifier
        if(system){
            identifier = new IdentifierDTO(code, value, system)
        } else {
            identifier = new IdentifierDTO(code, value)
        }
        identifierDTOList.add(identifier)

        return identifierDTOList
    }

    public Map fetchMedicationData(def clinicalDataObj){
        String[] medicationIdList =  clinicalDataObj?.get("resource")?.get("medicationReference")?.reference?.split("/")
        String medicationId = medicationIdList?.size() == 2 ? medicationIdList[1]: null
        if (medicationId && clinicalDataObj?.apiName?.equals("medicationAdministrationClinicalReference")){
            medicationId = "a-"+clinicalDataObj.practiceId+".medication-"+medicationId
        }
        if(medicationId) {
            BasicDBObject medicationSearchCriteria = new BasicDBObject("practiceId", clinicalDataObj?.practiceId).append("resource.id", medicationId).append("apiName", "medicationReference");
        Map medicationMap =  mongoService.search(medicationSearchCriteria, athenaConst.REFERENCE_COLLECTION)
        return medicationMap
        }else{
            return null
        }
    }

   public Map validateClinicalData(def clinicalDataObj){
       Map responseMap = [:]
       if (clinicalDataObj.get("apiName").equals("documentReferenceClinicalReference") && clinicalDataObj.resource?.content?.getAt(0)?.attachment?.extension?.getAt(0)?.url?.equals("http://hl7.org/fhir/StructureDefinition/data-absent-reason")){
           responseMap.put("isValid", false)
           responseMap.put("errorMessage", "contentDocument is Mandatory which is not present")
       }
       else if(clinicalDataObj.get("apiName").equals("medicationRequestClinicalReference") || clinicalDataObj.get("apiName").equals("medicationAdministrationClinicalReference")){
           Map medicationMap = fetchMedicationData(clinicalDataObj)
            if(medicationMap?.totalCount?.count>0){
               def medicationResource = medicationMap.objects.getAt(0)

               if(medicationResource.get("resource")?.get("code")?.get("coding")?.getAt(0)?.get("display") != null && medicationResource.get("resource")?.get("code")?.get("coding")?.getAt(0)?.get("code") != null){
                   responseMap.put("isValid", true)
               }
               else{
                   responseMap.put("isValid", false)
                   responseMap.put("errorMessage", "medicationId and medicationName not present")
               }
            }else{
                responseMap.put("isValid", false)
                responseMap.put("errorMessage", "medicationId and medicationName not present")
           }
       }
       else if(clinicalDataObj.get("apiName").equals(athenaConst?.OBSERVATION_CLINICALREFERENCE)){
           if(clinicalDataObj?.resource?.dataAbsentReason) {
               responseMap.put("isValid", false)
               responseMap.put("errorMessage", "Mandatory data absent : " + (clinicalDataObj?.resource?.dataAbsentReason?.text ?: clinicalDataObj?.resource?.dataAbsentReason?.coding?.getAt(0)?.display))
           }else if(clinicalDataObj?.resource?.status?.equals("final") && clinicalDataObj?.resource?.effectiveDateTime == null && clinicalDataObj?.resource?.issued == null){
               responseMap.put("isValid", false)
               responseMap.put("errorMessage","Mandatory fields both effectiveDateTime and issued are not present when status is final.")
           }else{
               responseMap.put("isValid", true)
           }
       }
       else if(clinicalDataObj.get("apiName").equals(athenaConst?.CLINICALIMPRESSION_CLINICALREFERENCE) && clinicalDataObj?.encounterId == null){
           responseMap.put("isValid", false)
           responseMap.put("errorMessage","Clinical Impression not linked with an encounter.")
       }
       else {
           responseMap.put("isValid", true)
       }
       return responseMap
    }

    public def getBinaryData(String binaryId,String practiceId){
        Map binaryApiConfig = athenaConst.getAPIConfig(athenaConst?.BINARY_CLINICALREFERENCE);
        def resultMap = athenaCallAPI(binaryApiConfig, ["logicalId":binaryId])?.result;
        if(resultMap) {
            Date currentDate = new Date();
            DBObject athenaDocument = new BasicDBObject();
            athenaDocument.put("retries", 0);
            athenaDocument.put("status", athenaConst.INITIATED);
            athenaDocument.put("dateCreated", currentDate);
            athenaDocument.put("lastUpdated", currentDate);
            athenaDocument.put("practiceId", practiceId);
            athenaDocument.put("apiName", athenaConst?.BINARY_CLINICALREFERENCE);

            athenaDocument.resource = resultMap;
            //Going to insert the document
            mongoService.save(athenaDocument.toJson(), athenaConst.REFERENCE_COLLECTION)
            return resultMap?.data
        }else{
            return null
        }
    }

    public String getDateBasedEncounterId(String date,String patientId,String practiceId){
        Date date1 = strToDateFormatTimeZoneConvert(date,null,athenaConf?.timezone)
        String dateString = dateToStrFormatTimeZoneConvert(date1,"yyyy-MM-dd'T'HH:mm:ss")
        BasicDBObject searchCriteria = new BasicDBObject("patientId",patientId).append("practiceId", practiceId).append("apiName", athenaConst.ENCOUNTER_REFERENCE).append("resource.period.start",new BasicDBObject("\$lte",dateString))
        BasicDBObject sortCriteria = new BasicDBObject("resource.period.start",-1)

        Map encounterObjs = mongoService.search(searchCriteria, athenaConst.REFERENCE_COLLECTION, sortCriteria);
        if(encounterObjs?.totalCount?.count>0){
            return encounterObjs?.objects?.getAt(0)?.encounterId
        }else{
            return null;;
        }
    }

    public String valueFormatter(String value,String splitKey,String joinKey,int startIndex){
        def valueSplitList = value.split(splitKey)
        value = null;
        for(int i=startIndex;i<valueSplitList.size();i++){
            value = value == null ? valueSplitList[i] : value +joinKey+valueSplitList[i]
        }
        return value
    }

    public QuantityDTO createQuantityDTO(Map quantityMap){
        QuantityDTO quantityDTO = new QuantityDTO();
        quantityDTO.unit = quantityMap?.unit
        quantityDTO.value = quantityMap?.value
        quantityDTO.code = quantityMap?.code
        quantityDTO.system = quantityMap?.system
        quantityDTO.comparator = quantityMap?.comparator

        return quantityDTO
    }
    public static JSONObject getSecret(String secretName) {
        JSONObject jsonObject = null;
        try {
            Region region = Region.of("us-east-2");
            SecretsManagerClient client = SecretsManagerClient.builder()
                    .region(region)
                    .build();
            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();
            GetSecretValueResponse getSecretValueResponse;

            getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
            String secret = getSecretValueResponse.secretString();

            jsonObject = new JSONObject(secret);
        } catch (Exception e) {
            log.error("Exception in getSecret : ",e);
        }
        return jsonObject;
    }
}