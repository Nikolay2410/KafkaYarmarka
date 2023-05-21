import fetch from "node-fetch";
import cors from "cors";
import express from "express";
import path from "path";
import { Kafka } from "kafkajs";
//ЯРМАРКА ПРОЕКТОВ
const PORT = 4000;

const kafka = new Kafka({
  clientId: "kafka",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "my-group" });

//Апи для получения архивных заявок студентов
const API_ARCHIVE_PARTICIPATIONS = "http://62.109.5.123/api/kampus"
//Массив ненужных ключей
const delKeys = [
'created_at', 
'updated_at', 
'places', 
'difficulty', 
'requirements ', 
'additional_inf', 
'state_id', 
'type_id',
'prev_project_id', 
'department_id', 
'theme_source_id', 
'rejection_reason', 
'rejection_date', 
'pivot', 
'specialities', 
'supervisors'];

const app = express();
app.use(cors());
app.use(express.static(path.join("client", "src")));
//ЯРМАРКА ПРОЕКТОВ

//Запись данных в топик CampusTopic
async function sendMessages(key, obj_value) {
  await producer.connect();
  await producer.send({
    topic: "CampusTopic",
    messages: [
      {
        key: key,
        value: obj_value,
      },
    ],
  });
  await producer.disconnect();
}

//Чтение сообщений из топика YarmarkaTopic
async function consumeMessages() {
  const messages = [];
 try {
  await consumer.connect();
  //await consumer.pause({ topic: "YarmarkaCampus", partitions: ["1"] });
  await consumer.subscribe({ topic: "YarmarkaTopic", fromBeginning: false});
  await new Promise(async (resolve) => {
    let timeout = setTimeout(resolve, 1000);
    consumer.run({
      eachMessage: ({ topic, partition, message }) => {
        clearTimeout(timeout);
        timeout = setTimeout(resolve, 200);
        messages.push(message.value.toString());
      },
    });
    //consumer.seek({ topic: "YarmarkaCampus", partition: 0});
  });
  //await consumer.resume({ topic: "YarmarkaCampus", partitions: ["1"] });
  await consumer.disconnect();
}
catch {
}
  return messages;
}

//Удаление ненужных ключей из JSON
function deleteKeysJSON (archive_parts_JSON, delKeysArr) {
  const newJSON = {...archive_parts_JSON};
  for (const item of delKeysArr) {
    delete newJSON[item];
  }
  //console.log(newJSON);
  return newJSON;
}

//Обработка JSON (получение нужных данных по заявкам студентов по проектам)
async function processArchiveParticipations() {
  console.log("НАЧАЛО ОБРАБОТКИ ЗАЯВОК");
  let resultJSON = await getData(API_ARCHIVE_PARTICIPATIONS);
  console.log("НАЧАЛО УДАЛЕНИЯ КЛЮЧЕЙ");
  resultJSON = resultJSON.map((item) => deleteKeysJSON(item,delKeys));
  console.log("ЗАВЕРШЕНИЕ УДАЛЕНИЯ КЛЮЧЕЙ");
  //console.log(resultJSON);
  console.log("ЗАВЕРШЕНИЕ ОБРАБОТКИ ЗАЯВОК");
  return resultJSON;
}

//Получение данных через api
async function getData(url) {
  const response = await fetch(url);
  const data = await response.json();
  //console.log("my getData JSON:")
  //console.log(JSON.stringify(data));
  return data;
}

//Обработка полученных сообщений от кампуса и отправка соответствующих в ответ
async function processMessages(messages) {
  for (const item of messages) {
    switch (item){
        case "get-archive":
          console.log("ОТПРАВКА ЗАЯВОК В КАМПУС");
          const strJSON = await processArchiveParticipations();
          console.log("ТУТ JSON");
          //console.log(strJSON);
          await sendMessages("1", JSON.stringify(strJSON));
          console.log("ЗАВЕРШЕНИЕ ОТПРАВКИ ЗАЯВОК В КАМПУС");
          break;
        default:
    }
  }
}

//ЯРМАРКА ПРОЕКТОВ
app.get("/api/messages", async function (req, res) {
  console.log("Ярмарка проектов");
  try {
    if (req.query.obj) {
      var message = "";
      if (req.query.message) {
        message = req.query.message;
      }
      console.log("message: ", message);
      switch (req.query.obj) {
        case "send1":
          console.log("Отправка");
          await sendMessages("1", message);
          break;
        case "get1":
          console.log("Получка");
          const getMessages = await consumeMessages();
          console.log("messages from Camp: " + getMessages);
          processMessages(getMessages);
          res.send(getMessages);
          break;
        default:
          console.log("Other:", messages);
      }
    }
  } catch (error) {
    res.send(error).status(400);
  }
});

app.listen(PORT, () => {
  console.log(`server started http://localhost:${PORT}`);
});

