const _ = require('lodash');
const request = require('request');
const csv=require('csvtojson');
const joi  = require('@hapi/joi');
const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { default: axios } = require('axios');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors())
app.use(bodyParser.json());


const errorMessage = ({
    res,
    msg = 'No csv parameter (csv) was provided in body, csv cannot be parsed, please provide a csv in the format {csv: {url: "http://xxx.com"}}',
    error = 'No csv parameter (csv) was provided in body, csv cannot be parsed, please provide a csv in the format {csv: {url: "http://xxx.com"}}',
    status = 400
}) => {
    res.status(status).json({
        success: false,
        message: msg,
        error: error
    });
}

const returnErrorFromSchema = (value, options) => {
    const csvParseSchema = joi.object().keys({
        url: joi.string().uri().required().error(error => {
            error.forEach(err => {
                switch(err.code) {
                    case 'string.empty': 
                        err.message = 'Url cannot be empty';
                        break;
                    case 'string.uri':
                        err.message = 'Provide a valid url to csv';
                        break;
                    default:
                        break;
                }
            })
            return error;
        }),
        select_fields: joi.array().items(joi.string()).required().error(error => {
            return error;
        })
    });

    const csvToParse = joi.object().keys({
        csv: csvParseSchema.required().error(error => {
            return error;
        })
    }).required().error(error => {
        return error;
    });

    const { error } = csvToParse.validate(value, options);
    return error;
    
}

const validateSchema = (req, res, next) => {
    const error = returnErrorFromSchema(req.body, { abortEarly: false });
    if (error) {
      const errors = {};
      error.details.forEach((err) => {
        const newError = err;
        errors[newError.context.key] = newError.message;
      });
      return errorMessage({res, status: 400, msg: 'Validate Schema', error: errors});
    }
    return next();
}



app.get('/', function (req, res) {
  res.send('Hello World!');
});

app.post('/', validateSchema, async (req, res) => {
    try {
        const body = req.body;
        const csvToParse = body.csv;

        let checkIfCsv = await axios.get(csvToParse.url);
        if (!(String(checkIfCsv.headers['content-type']).includes('csv'))) throw new Error("Provide a link to a csv file");

        let x = [];

        csv()
        .fromStream(request.get(csvToParse.url))
        .then((json, err) => {
            if (err) throw new Error(err.message || "Something went wrong");
            
            const conversion_key = uuidv4();
            const fields = csvToParse.select_fields;

            if (_.isEmpty(json)) return res.status(501).json({success: false, message: 'csv file is empty'});

            if (!(fields.length > 0)) return res.status(200).json({ conversion_key, json });

            let newJson = [];

            json.forEach(item => {
                if (_.isObject(item)) {
                    const temp = {};
                    Object.keys(item).forEach(key => {
                        if (fields.includes(key)) temp[key] = item[key];
                    });
                    if (!_.isEmpty(temp)) newJson.push(temp);
                } else {
                    newJson.push(item);
                }
            });


            const getKeys  = () => {
                const findKeys = json[json.length - 1];
                const keys = Object.keys(findKeys).map(i => i).toString();
                return keys;
            }

            return res.status(newJson.length > 0 ? 200 : 501).json({
                conversion_key : newJson.length > 0 ? conversion_key : '',
                json: newJson.length > 0 ? newJson : `field does not exists, try fields like \'${getKeys()}\'`
            });

        })

    } catch (e) {
        console.log(e);
        errorMessage({res, msg: e.message, error: e.message});
    }
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});