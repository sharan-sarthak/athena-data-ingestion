package com.mphrx.core

import com.mphrx.util.grails.ApplicationContextUtil
import grails.util.Holders
import org.apache.log4j.Logger
import grails.transaction.Transactional

@Transactional
class AthenaConstantsAndAPIConfigService {
    public static Logger log = Logger.getLogger("com.mphrx.AthenaConstantsAndAPIConfigService")
    public static final LEAVE_UNPROCESSED = ApplicationContextUtil.getConfig().athena?.leaveunprocessed

    //All static constants going to be used throughout all the Athena framework.
    //General Static Constants
    public static final SUCCESS = "Success";
    public static final FAILURE = "Failure";
    public static final INVALID_TOKEN = "Invalid Token";
    public static final INVALID_METHOD = "Invalid HTTP Method";
    public static final GET = "GET";
    public static final POST = "POST";
    public static final SUBSCRIPTION = "subscription"; //Multiple Records Queue Based. Once Fetched, It will be deleted from source.
    public static final REFERENCE = "reference"; //Single Records. Always persist at source source.
    public static final CLINICALREFERENCE = "clinicalReference";
    public static final SEARCH = "search"; //Multiple Records. Always persist at source source.
    public static final GENERIC = "generic"; //Generic APIs. These APIs will be used as is.
    public static final STAGING_COLLECTION = "athenaStaging";
    public static final REFERENCE_COLLECTION = "athenaReferenceData"
    public static final HL7 = "Hl7";
    public static final SINGLE = "SINGLE";
    public static final MULTIPLE = "MULTIPLE";
    public static final LIST = "List";
    public static final STRING = "String";
    public static final REFERENCE_HL7 = "refrenceHL7";
    public static final CLINICAL_HL7 = "clinicalHL7";

    //routeToAction Names
    public static final FETCH_SUBSCRIPTION_DATA = "fetchSubscriptionData";
    public static final FETCH_REFERENCE_DATA_FOR_STAGING = "fetchReferenceDataForStaging"; //Used when Staging data populated.
    public static final FETCH_CLINICAL_REFERENCE_DATA = "fetchClinicalReferenceData"; //Used when clinical data populated.
    public static final FETCH_REFERENCE_DATA = "fetchReferenceData"; //Used to fetch only one reference data //Not in use
    public static final FETCH_SEARCH_DATA = "fetchSearchData";
    public static final FETCH_CCDA_DATA = "fetchCcdaData";
    public static final CREATE_HL7_DATA_FROM_STAGING = "createHl7DataFromStaging";
    public static final INGEST_MASTER_DATA = "ingestMasterData";
    public static final FETCH_HISTORICAL_DATA = "fetchHistoricalData";

    //staging collection status values
    public static final PENDING = "PENDING";
    public static final INITIATED = "INITIATED";
    public static final CDF = "CLINICAL_DATA_FETCHED";
    public static final CDFF = "CLINICAL_DATA_FETCH_FAILED";
    public static final PROCESSED = "PROCESSED"
    public static final PROCESSING_REFERENCES = "PROCESSING_REFERENCES";
    public static final FAILED_REFERENCES = "FAILED_REFERENCES";
    public static final FAILED = "FAILED";
    public static final READYFORINGEST = "READYFORINGEST";
    public static final PROCESSING_HL7 = "PROCESSING_HL7";
    public static final PROCESSED_HL7 = "PROCESSED_HL7";
    public static final PARTIAL_HL7 = "PARTIAL_HL7";
    public static final FAILED_HL7 = "FAILED_HL7";
    public static final ACTIVE = "active";
    public static final INACTIVE = "inactive";
    public static final PROCESSING = "PROCESSING";
    public static final NO_DATA_FOUND = "NO_DATA_FOUND";

    //All Subscription Based APIs And Related Constants
    public static final PATIENT_SUBSCRIPTION = "patientSubscription";
    public static final APPOINTMENT_SUBSCRIPTION = "appointmentSubscription";
    public static final APPOINTMENT_CSV_DATA = "appointmentCSVData";
    public static final ENCOUNTER_SUBSCRIPTION = "encounterSubscription";
    public static final PROVIDER_SUBSCRIPTION = "providerSubscription";
    //All Reference Based APIs And Related Constants
    public static final PRACTICE_REFERENCE = "practiceReference";
    public static final DEPARTMENT_REFERENCE = "departmentReference";
    public static final PATIENT_REFERENCE = "patientReference";
    public static final ENCOUNTER_REFERENCE = "encounterReference";
    public static final PROVIDER_REFERENCE = "providerReference";
    public static final APPOINTMENT_REFERENCE = "appointmentReference";
    public static final ENCOUNTER_HISTORICAL_REFERENCE = "encounterHistoricalReference"
    public static final MEDICATION_REFERENCE = "medicationReference";

    //All Search/FetchALL Based APIs And Related Constants
    public static final PRACTICE_SEARCH = "practiceSearch";
    public static final DEPARTMENT_SEARCH = "departmentSearch";
    public static final PROVIDER_SEARCH = "providerSearch";
    public static final PRACTICEINFO_SEARCH = "practiceInfoSearch"

    //All Clinical Based APIS And Related Constants
    public static final CONDITION_DIAGNOSIS_CLINICALREFERENCE = "conditionDiagnosisClinicalReference";
    public static final CONDITION_PROBLEM_CLINICALREFERENCE = "conditionProblemClinicalReference";
    public static final MEDICATIONREQUEST_CLINICALREFERENCE = "medicationRequestClinicalReference";
    public static final MEDICATIONADMINISTRATION_CLINICALREFERENCE = "medicationAdministrationClinicalReference";
    public static final DIAGNOSTICREPORT_CLINICALREFERENCE = "diagnosticReportClinicalReference";
    public static final OBSERVATION_CLINICALREFERENCE = "observationClinicalReference";
    public static final OBSERVATION_REPORT_CLINICALREFERENCE = "observationReportClinicalReference"
    public static final CLINICALIMPRESSION_CLINICALREFERENCE = "clinicalImpressionClinicalReference";
    public static final DOCUMENTREFERENCE_CLINICALREFERENCE = "documentReferenceClinicalReference";
    public static final SERVICEREQUEST_CLINICALREFERENCE = "serviceRequestClinicalReference";
    public static final PROCEDURE_CLINICALREFERENCE = "procedureClinicalReference";
    public static final IMMUNIZATION_CLINICALREFERENCE = "immunizationClinicalReference";
    public static final ALLERGYINTOLERANCE_CLINICALREFERENCE = "allergyIntoleranceClinicalReference"
    public static final GOAL_CLINICALREFERENCE = "goalClinicalReference"
    public static final BINARY_CLINICALREFERENCE = "binaryClinicalReference"

    //Generic APIs
    public static final TOKEN_API = "tokenApi";

    //All HL7 Messages Type Used For Athena
    public static final ADT = "ADT";
    public static final SIU = "SIU";
    public static final MFN = "MFN";
    public static final VXU = "VXU";

    //All HL7 event Type Used For Athena
    public static final ADT_A01 = "A01";
    public static final ADT_A04 = "A04";
    public static final ADT_A08 = "A08";
    public static final SIU_S12 = "S12";
    public static final MFN_M02 = "M02";

    //Athena API Constants
    public static final Map ATHENA_APPOINTMENT_STATUS= ["x":"cancelled", "f":"proposed", "o":"booked", "2": "arrived", "3":"fulfilled","4":"fulfilled"];

    //Relationship map
    public static final Map RELATIONSHIP_MAPPING = ["1":"Self", "2":"Spouse", "3":"Child", "4":"Other", "5":"Grandparent", "6":"Grandchild", "7":"Nephew or Niece", "9":"Foster Child", "10":"Ward", "11":"Stepson or Stepdaughter", "12":"Employee", "13":"Unknown", "14":"Handicapped Dependent", "15":"Sponsored Dependent", "16":"Dependent of a Minor Dependent", "17":"Significant Other", "18":"Mother", "19":"Father", "21":"Emancipated Minor", "22":"Organ Donor", "23":"Cadaver Donor", "24":"Injured Plaintiff", "25":"Child (Ins. not Financially Respons.)", "26":"Life Partner", "27":"Child (Mother's Insurance)", "28":"Child (Father's Insurance)", "29":"Child (Mother's Ins., Ins. not Financially Respons.)", "30":"Child (Father's Ins., Ins. not Financially Respons.)", "31":"Stepson or Stepdaughter (Stepmother's Insurance)", "32":"Stepson or Stepdaughter (Stepfather's Insurance)"]

    /*
        Global Map which contains the Metadata for each APIs and will be used in various processes to each the
        workflow and reduce the dependency.
        apiType Subscription and Search will return hash of array of hash response where array of hash data exist with a key and totalcount. eg. {"totalcount": 2,"practiceinfo": [{"practiceId":"10"},{"practiceId":"11"}]
        apiType reference will return the array of hash response. We except single data for these APIs. eg. [{"patientid": "20"}]
     */
    public static Map AthenaApiMap = [
            (TOKEN_API) : [
                    "apiType": GENERIC,
                    "apiMethod": POST,
                    "apiEndPoint": "/token", //No default practiceId prefix required
                    "apiParams": [:],
                    "apiStaticParams": [:],
                    "primaryIdKey": "",
                    "apiResponseKey": "access_token",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : ""
            ],// @TODO to make this dynamic
            (PATIENT_SUBSCRIPTION) : [
                    "apiType": SUBSCRIPTION, //Define the API Nature (Subscription-> Queue Based available for 24 hr, Reference-> Real time always available)
                    "apiMethod": GET, //HTTP Method to call the API
                    "apiEndPoint": "/v1/practiceId/patients/changed", //Same API with patients/changed/subscription will be used to enable/check the subscription APIs.
                    "apiParams": [:], //Params use in API URL. Same needs to be replaced by actual values during API URL building. No use in Subscription APIs
                    "apiStaticParams": ["limit": "20", "returnglobalid" : "true", "showpreviouspatientids" : "true", "leaveunprocessed" : LEAVE_UNPROCESSED], //Static params with values, Which needs to define as is in URL or Body request depending in the apiMethod.
                    "primaryIdKey": "patientid", //Primary Key received in each response object. Same will be used to store in DB
                    "apiResponseKey": "patients", //This is the response Key which hold the all received object for bulk serach. For this will be used only for subscription/search
                    "otherReferenceApis":[(DEPARTMENT_REFERENCE):["keyName":"departmentid", "type":STRING],
                                          (PROVIDER_REFERENCE):["keyName":"primaryproviderid", "type":STRING]
                    ],//Other attached references, for them we have to get the data once subscription have the initial data inplace. Here keyName and type defined the nature of response in dataset
                    "messageType": [(ADT):ADT_A04], //Will be used to create final dataset for Hl7Message collection.
                    "modField" : "dob", //Required for jobInstanceNumber
            ],
            (PATIENT_REFERENCE) : [
                    "apiType": REFERENCE, //Define the API Nature (Subscription-> Queue Based available for 24 hr, Reference-> Real time always available)
                    "apiMethod": GET, //HTTP Method to call the API
                    "apiEndPoint": "/fhir/r4/Patient", //Same API with patients/changed/subscription will be used to enable/check the subscription APIs.
                    "apiParams": ["_id":["patientid"],"ah-practice":["ah-practice"]], //Params use in API URL. Same needs to be replaced by actual values during API URL building. Here key have the alias name present with other ref in API response
                    "apiStaticParams": [:], //Static params with values, Which needs to define as is in URL or Body request depending in the apiMethod.
                    "primaryIdKey": "patientid", //Primary Key received in each response object. Same will be used to store in DB
                    "apiResponseKey": "",
                    "otherReferenceApis":[(DEPARTMENT_REFERENCE):["keyName":"departmentid", "type":STRING],
                                          (PROVIDER_REFERENCE):["keyName":"primaryproviderid", "type":STRING]
                    ],//Other attached references, for them we have to get the data once subscription have the initial data inplace. Here keyName and type defined the nature of response in dataset
                    "messageType": [:], //Will be used to create final dataset for Hl7Message collection. Not required for reference APIs
                    "modField" : "dob"
            ],
            (PRACTICE_SEARCH) : [
                    "apiType": SEARCH,
                    "apiMethod": GET,
                    "apiEndPoint": "/1/practiceinfo", //No default practiceId prefix required
                    "apiParams": [:],
                    "apiStaticParams": [:],
                    "primaryIdKey": "practiceid",
                    "apiResponseKey": "practiceinfo",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : ""
            ],
            (PRACTICE_REFERENCE) : [
                    "apiType": REFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/practiceid/practiceinfo", //No default practiceId prefix required
                    "apiParams": ["practiceid":["practiceid"]],
                    "apiStaticParams": [:],
                    "primaryIdKey": "practiceid",
                    "apiResponseKey": "",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : ""
            ],
            (PRACTICEINFO_SEARCH) : [
                    "apiType": SEARCH,
                    "apiMethod": GET,
                    "apiEndPoint": "/v1/practiceId/practiceinfo",
                    "apiParams": [:],
                    "apiStaticParams": [:],
                    "primaryIdKey": "practiceid",
                    "apiResponseKey": "practiceinfo",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : "",
                    "masterType" : "PRACTICEINFO"
            ],
            (DEPARTMENT_SEARCH) : [
                    "apiType": SEARCH,
                    "apiMethod": GET,
                    "apiEndPoint": "/v1/practiceId/departments",
                    "apiParams": [:],
                    "apiStaticParams": ["providerlist":"false","showalldepartments":"false"],
                    "primaryIdKey": "departmentid",
                    "apiResponseKey": "departments",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : "",
                    "masterType" : "DEPARTMENT"
            ],
            (DEPARTMENT_REFERENCE) : [
                    "apiType": REFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/departments/departmentid",
                    "apiParams": ["departmentid":["departmentid"]],
                    "apiStaticParams": ["providerlist":"false", "showalldepartments":"false"],
                    "primaryIdKey": "departmentid",
                    "apiResponseKey": "",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : ""
            ],
            (PROVIDER_REFERENCE) : [
                    "apiType": REFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Practitioner",
                    "apiParams": ["ah-practice":["ah-practice"],"_id":["providerid"]],
                    "apiStaticParams": [:],
                    "primaryIdKey": "providerid",
                    "apiResponseKey": "",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : "npi"
            ],
            (APPOINTMENT_SUBSCRIPTION) : [
                    "apiType": SUBSCRIPTION,
                    "apiMethod": GET,
                    "apiEndPoint": "/v1/practiceId/appointments/changed",
                    "apiParams": [:],
                    "apiStaticParams": ["limit": "20", "leaveunprocessed" : LEAVE_UNPROCESSED, "showclaimdetail": "false", "showcopay":"false", "showinsurance": "false", "showremindercalldetail":"false"],
                    "primaryIdKey": "appointmentid",
                    "apiResponseKey": "appointments",
                    "otherReferenceApis":[(DEPARTMENT_REFERENCE):["keyName":"departmentid", "type":STRING],
                                          (PROVIDER_REFERENCE):["keyName":"providerid", "type":STRING],
                                          (PATIENT_REFERENCE):["keyName":"patientid", "type":STRING],
                                          (ENCOUNTER_REFERENCE):["keyName":"encounterid", "type":STRING]
                    ],
                    "messageType": [(SIU) :SIU_S12], //Will do the special handling  for SIU with encounter diagnosis. We will create extra ADT
                    "modField" : "patientid"
            ],
            (ENCOUNTER_REFERENCE) : [
                    "apiType": REFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Encounter",
                    "apiParams": ["_id":["encounterid"],"ah-practice":["ah-practice"]],
                    "apiStaticParams": ["date":ApplicationContextUtil.getConfig().athena.encounterDateParam],
                    "primaryIdKey": "encounterid",
                    "apiResponseKey": "",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : "encounterid"
            ],
            (ENCOUNTER_HISTORICAL_REFERENCE) : [
                    "apiType": REFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Encounter",
                    "apiParams": ["patient":["patientid"],"ah-practice":["ah-practice"]],
                    "apiStaticParams": ["date":ApplicationContextUtil.getConfig().athena.encounterDateParam],
                    "primaryIdKey": "patientid",
                    "apiResponseKey": "",
                    "otherReferenceApis":[],
                    "messageType": [:],
                    "modField" : "patientid"
            ],
            (CONDITION_DIAGNOSIS_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Condition",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"],"encounter":["encounter"]],
                    "apiStaticParams": ["category":"encounter-diagnosis"]
            ],
            (CONDITION_PROBLEM_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Condition",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"]],
                    "apiStaticParams": ["category":"problem-list-item"]
            ],
            (MEDICATIONREQUEST_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/MedicationRequest",
                    "_include": "MedicationRequest:requester:Practitioner",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"],"encounter":["encounter"]],
                    "apiStaticParams": ["intent":"proposal,plan,order,reflex-order,original-order,filler-order,instance-order,option"]
            ],
            (MEDICATIONADMINISTRATION_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/v1/practiceId/departmentId/fhir/dstu2/MedicationAdministration",
                    "apiParams": ["patient":["patientId"]],
                    "apiStaticParams": [:]
            ],
            (SERVICEREQUEST_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/ServiceRequest",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"]],
                    "apiStaticParams": [:]
            ],
            (OBSERVATION_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Observation",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"],"encounter":["encounter"]],
                    "apiStaticParams": [:]
            ],
            (OBSERVATION_REPORT_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Observation",
                    "apiParams": ["ah-practice":["ah-practice"],"_id":["observationId"]],
                    "apiStaticParams": [:]
            ],
            (CLINICALIMPRESSION_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/v1/practiceId/departmentId/fhir/dstu2/ClinicalImpression",
                    "apiParams": ["patient":["patientId"]],
                    "apiStaticParams": [:]
            ],
            (DOCUMENTREFERENCE_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/DocumentReference",
                    "_include":"DocumentReference:author:Practitioner",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"],"encounter":["encounter"]],
                    "apiStaticParams": [:]
            ],
            (DIAGNOSTICREPORT_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/DiagnosticReport",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"]],
                    "apiStaticParams": [:]
            ],
            (PROCEDURE_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Procedure",
                    "_include" : "Procedure:performer:Practitioner",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"],"encounter":["encounter"]],
                    "apiStaticParams": [:]
            ],
            (IMMUNIZATION_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Immunization",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"]],
                    "apiStaticParams": ["date":ApplicationContextUtil.getConfig().athena.encounterDateParam]
            ],
            (ALLERGYINTOLERANCE_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/AllergyIntolerance",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"]],
                    "apiStaticParams": [:]
            ],
            (GOAL_CLINICALREFERENCE) : [
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Goal",
                    "apiParams": ["ah-practice":["ah-practice"],"patient":["patient"]],
                    "apiStaticParams": [:]
            ],
            (MEDICATION_REFERENCE) : [
                    "apiType": REFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Medication",
                    "apiParams": ["ah-practice":["ah-practice"],"_id":["medicationId"]],
                    "apiStaticParams": [:]
            ],
            (BINARY_CLINICALREFERENCE):[
                    "apiType": CLINICALREFERENCE,
                    "apiMethod": GET,
                    "apiEndPoint": "/fhir/r4/Binary/logicalId",
                    "apiParams": [:],
                    "apiStaticParams": [:]
            ]
    ]

    /*
    Get All Subscription APIs
     */
    public Map getAllSubscriptionAPIs(){
        Map subscriptionApiMap = AthenaApiMap.findAll {k,v -> v.apiType.toString() == SUBSCRIPTION};
        return new LinkedHashMap(subscriptionApiMap);
    }

    /*
    Get All Master Data APIs
     */
    public Map getAllMasterConfigs(){
        Map masterApiMap = AthenaApiMap.findAll {k,v -> v.containsKey("masterType")};
        return new LinkedHashMap(masterApiMap);
    }

    /*
        Get a Particular API Config
    */
    public Map getAPIConfig(String apiName){
        Map apiConfigMap = AthenaApiMap.find {k,v -> k.toString() == apiName}.value;
        return new LinkedHashMap(apiConfigMap);
    }

    /*
    Get All Clinical Reference APIs
     */
    public Map getAllClinicalAPIs(){
        Map clinicalApiMap = AthenaApiMap.findAll {k,v -> v.apiType.toString() == CLINICALREFERENCE};
        return new LinkedHashMap(clinicalApiMap);
    }
}


