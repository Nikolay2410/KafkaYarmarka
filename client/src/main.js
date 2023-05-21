async function fetchMessages(obj) {
  str = "/api/messages";
  switch (obj) {
    case "send1":
      str += "?obj=send1";
      break;
    case "get1":
      str += "?obj=get1";
      break;
    default:
      str += "/";
  }
  if (obj.substring(0,3) != "get") {
    if (inp1.value != "") {
      str += "&message=" + inp1.value;
    }
  }

  const res = await fetch(str);
  if (!res.ok) throw new Error(res.statusText);
  return res.json();
}

const inp1 = document.getElementById("inp1");
const btnSys1Get = document.getElementById("sys1_get");
const btnSys1Send = document.getElementById("sys1_send");
const sys1Messages = document.getElementById("sys1_messages");

btnSys1Send.addEventListener("click", async () => {
  if (inp1.value != "") {
    try {
      sys1Messages.innerHTML = "Отправка данных в Кампус...";
      await fetchMessages("send1");
      sys1Messages.innerHTML = "Данные отправлены";
    } catch (error) {
      sys1Messages.innerHTML = String(error);
    }
  }
});

btnSys1Get.addEventListener("click", async () => {
  try {
    sys1Messages.innerHTML = "Получение данных от Кампуса...";
    const messages = await fetchMessages("get1");
    sys1Messages.innerHTML = JSON.stringify(messages, null, 2);
  } catch (error) {
    sys1Messages.textContent = String(error);
  }
});









