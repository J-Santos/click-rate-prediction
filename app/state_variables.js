var queryModel    =   require("./models/query");

var functionAL = function(callback){
    queryModel.selectQueryByState('AL', callback)
}
var functionAK = function(callback){
    queryModel.selectQueryByState('AK', callback)
}
var functionAZ = function(callback){
    queryModel.selectQueryByState('AZ', callback)
}
var functionAR = function(callback){
    queryModel.selectQueryByState('AR', callback)
}
var functionCA = function(callback){
    queryModel.selectQueryByState('CA', callback)
}
var functionCO = function(callback){
    queryModel.selectQueryByState('CO', callback)
}
var functionCT = function(callback){
    queryModel.selectQueryByState('CT', callback)
}
var functionDE = function(callback){
    queryModel.selectQueryByState('DE', callback)
}
var functionDC = function(callback){
    queryModel.selectQueryByState('DC', callback)
}
var functionFL = function(callback){
    queryModel.selectQueryByState('FL', callback)
}
var functionGA = function(callback){
    queryModel.selectQueryByState('GA', callback)
}
var functionHI = function(callback){
    queryModel.selectQueryByState('HI', callback)
}
var functionID = function(callback){
    queryModel.selectQueryByState('ID', callback)
}
var functionIL = function(callback){
    queryModel.selectQueryByState('IL', callback)
}
var functionIN = function(callback){
    queryModel.selectQueryByState('IN', callback)
}
var functionIA = function(callback){
    queryModel.selectQueryByState('IA', callback)
}
var functionKS = function(callback){
    queryModel.selectQueryByState('KS', callback)
}
var functionKY = function(callback){
    queryModel.selectQueryByState('KY', callback)
}
var functionLA = function(callback){
    queryModel.selectQueryByState('LA', callback)
}
var functionME = function(callback){
    queryModel.selectQueryByState('ME', callback)
}
var functionMD = function(callback){
    queryModel.selectQueryByState('MD', callback)
}
var functionMA = function(callback){
    queryModel.selectQueryByState('MA', callback)
}
var functionMI = function(callback){
    queryModel.selectQueryByState('MI', callback)
}
var functionMN = function(callback){
    queryModel.selectQueryByState('MN', callback)
}
var functionMS = function(callback){
    queryModel.selectQueryByState('MS', callback)
}
var functionMO = function(callback){
    queryModel.selectQueryByState('MO', callback)
}
var functionMT = function(callback){
    queryModel.selectQueryByState('MT', callback)
}
var functionNE = function(callback){
    queryModel.selectQueryByState('NE', callback)
}
var functionNV = function(callback){
    queryModel.selectQueryByState('NV', callback)
}
var functionNH = function(callback){
    queryModel.selectQueryByState('NH', callback)
}
var functionNJ = function(callback){
    queryModel.selectQueryByState('NJ', callback)
}
var functionNM = function(callback){
    queryModel.selectQueryByState('NM', callback)
}
var functionNY = function(callback){
    queryModel.selectQueryByState('NY', callback)
}
var functionNC = function(callback){
    queryModel.selectQueryByState('NC', callback)
}
var functionND = function(callback){
    queryModel.selectQueryByState('ND', callback)
}
var functionOH = function(callback){
    queryModel.selectQueryByState('OH', callback)
}
var functionOK = function(callback){
    queryModel.selectQueryByState('OK', callback)
}
var functionOR = function(callback){
    queryModel.selectQueryByState('OR', callback)
}
var functionPA = function(callback){
    queryModel.selectQueryByState('PA', callback)
}
var functionPR = function(callback){
    queryModel.selectQueryByState('PR', callback)
}
var functionRI = function(callback){
    queryModel.selectQueryByState('RI', callback)
}
var functionSC = function(callback){
    queryModel.selectQueryByState('SC', callback)
}
var functionSD = function(callback){
    queryModel.selectQueryByState('SD', callback)
}
var functionTN = function(callback){
    queryModel.selectQueryByState('TN', callback)
}
var functionTX = function(callback){
    queryModel.selectQueryByState('TX', callback)
}
var functionUT = function(callback){
    queryModel.selectQueryByState('UT', callback)
}
var functionVT = function(callback){
    queryModel.selectQueryByState('VT', callback)
}
var functionVI = function(callback){
    queryModel.selectQueryByState('VI', callback)
}
var functionVA = function(callback){
    queryModel.selectQueryByState('VA', callback)
}
var functionWA = function(callback){
    queryModel.selectQueryByState('WA', callback)
}
var functionWV = function(callback){
    queryModel.selectQueryByState('WV', callback)
}
var functionWI = function(callback){
    queryModel.selectQueryByState('WI', callback)
}
var functionWY = function(callback){
    queryModel.selectQueryByState('WY', callback)
}

var state_func = [
    functionAL, functionAK, functionAZ, functionAR, functionCA, functionCO, functionCT, functionDE, functionDC, functionFL,
    functionGA, functionHI, functionID, functionIL, functionIN, functionIA, functionKS, functionKY, functionLA, functionME,
    functionMD, functionMA, functionMI, functionMN, functionMS, functionMO, functionMT, functionNE, functionNV, functionNH,
    functionNJ, functionNM, functionNY, functionNC, functionND, functionOH, functionOK, functionOR, functionPA, functionPR,
    functionRI, functionSC, functionSD, functionTN, functionTX, functionUT, functionVT, functionVI, functionVA, functionWA,
    functionWV, functionWI, functionWY
]

var state_func_test = [
    functionAL, functionAK, functionAZ, functionAR, functionCA
]

var state_map = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District Of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VI": "Virgin Islands",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming"
}

exports.state_func = state_func
exports.state_func_test = state_func_test
exports.state_map = state_map
