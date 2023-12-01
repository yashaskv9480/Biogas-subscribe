const express = require('express');
const mqtt = require('mqtt');
const { Pool } = require('pg');
const dotenv = require('dotenv');

dotenv.config();

const app = express();
const port = process.env.PORT || 3004;

const pool = new Pool({
  user: process.env.DB_USER || 'postgres',
  host: process.env.DB_HOST || 'localhost',
  database: process.env.DB_NAME || 'biogas',
  password: process.env.DB_PASSWORD || 'postgres',
  port: process.env.DB_PORT || 5432,
});

const clientId = 'c154';
const mqttClient = mqtt.connect(process.env.MQTT_BROKER_URL, {
  clientId,
  username: process.env.MQTT_USERNAME || 'biogas',
  password: process.env.MQTT_PASSWORD || 'BIO1234',
  port: process.env.MQTT_BROKER_PORT || 1883,
});


mqttClient.on('connect', () => {
  console.log('Connected to MQTT broker');

  mqttClient.subscribe('biogas', (err) => {
    if (!err) {
      console.log(`Subscribed to topic: biogas`);
    } else {
      console.error(`Error subscribing to topic: ${err}`);
    }
  });
});

mqttClient.on('message', async (receivedTopic, message) => {
  try {
    const messageObj = JSON.parse(message);
    console.log(messageObj)

    const deviceQuery = 'INSERT INTO DEVICE (DEVICE_ID) VALUES ($1) ON CONFLICT (DEVICE_ID) DO NOTHING';
    const deviceInsertValues = [messageObj.ID];
    await pool.query(deviceQuery, deviceInsertValues);

    const sensorParameterQuery = 'SELECT SLAVE_ID, REG_ADD FROM SENSOR_PARAMETERS WHERE DEVICE_ID = $1';
    const sensorParameterValues = [messageObj.ID];
    const sensorParameterResult = await pool.query(sensorParameterQuery, sensorParameterValues);

    const result2 = sensorParameterResult.rows.map((row) => ({
      slave_id: row.slave_id,
      reg_add: row.reg_add,
    }));
    console.log(result2);

    if (sensorParameterResult.rows.length === 0) {
      console.error('No sensor parameters found for device ID:', messageObj.ID);
      return;
    }

    const slaveId = messageObj.SL_ID;
    const regAdd = messageObj.RegAd;
    let insertValue=messageObj.D1;
    const device_id=messageObj.ID;
    const d_time = messageObj.DATE +" "+messageObj.TIME;

    

    const sensorParameters = result2.find(
      (param) => param.slave_id === slaveId && param.reg_add === regAdd
    );

    if (!sensorParameters) {
      console.error('No matching sensor parameters found for device ID, SL_ID, and RegAd:', messageObj.ID, slaveId, regAdd);
      return;
    }

    const sensorId = sensorParameters.slave_id; // Considering SLAVE_ID as SENSOR_ID
    console.log(sensorId,regAdd,JSON.stringify(insertValue))
    console.log(d_time)

    if (sensorId === "2" && regAdd === "2") {

      insertValue /= 100;
    }

    const insertQuery = `
      INSERT INTO SENSOR_VALUE (DEVICE_ID, SLAVE_ID, REG_ADD, VALUE, U_TIME,D_Time)
      VALUES ($1, $2, $3, $4, NOW(),$5)
    `;

    const insertValues = [device_id, sensorId, regAdd, insertValue,d_time];
    const result = await pool.query(insertQuery, insertValues);

    console.log('Inserted into the database:', result.rows[0]);
  } catch (err) {
    console.error('Error processing MQTT message:', err);
  }
});

mqttClient.on('error', (error) => {
  console.error(`MQTT Error: ${error}`);
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});