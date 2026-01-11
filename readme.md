# Monapps V5 - short description

This code represents a proof-of-concept version of a dedicated backend application that serves as a data collection environment and executes application functions designed to generate insights based on that data. Detailed information can be found in the [documentation](https://1drv.ms/f/c/d5a450febe052abb/IgDNEYUMorrNRIFrMlx0FIVJAdzSZM0VHofwtdzYhCB0kyc?e=wiuUZb).

![Datastream readings](img/dsreadings.jpg)

![Tree](img/tree.jpg)


## Setup
* Clone the project from GitHub.
* Rename the `.example` files in the folder `docker/dev/env`  by removing `.example` from their names.
* Replace all the variables in `<>` with your own values. Use the same value for `POSTGRES_DB` and `POSTGRES_USER`. When generating a Django `SECRET_KEY`, use the set of commands 
```shell
python manage.py shell
from django.core.management.utils import get_random_secret_key
print(get_random_secret_key())
```
You can alternatively use online resources like `https://djecrety.ir/` or `https://django-secret-key-generator.netlify.app/`.

* Change the "secret" in the file `docker/chirpstack/chirpstack/chirpstack.toml` in the `[api]` section by using the command `openssl rand -base64 32`

* If you want the application to start with some readings prepopulated to see the API and frontend app in work, use the file `extended_fixtures.json` in the [folder](https://1drv.ms/f/c/d5a450febe052abb/IgDNEYUMorrNRIFrMlx0FIVJAdzSZM0VHofwtdzYhCB0kyc?e=wiuUZb). Rename it to `fixtures.json` and replace the original file `fixtures.json` in the folder `monapps` before launching the containers.

## Start 
* Go to the `monapps` folder and start Docker compose for the first time with the flag `--build`

```shell
docker compose up -d --build
```
To stop the bundle, execute

```shell
docker compose down
```

For the sake of convenience, there are three Docker Compose files: `mosquitto.yml`, `chirpstack.yml` and `monapps-dev.yml` in addition to the original `docker-compose.yml` file. These files, if launched one by one, provide the same functionality as the initial `docker-compose.yml` file but give some flexibility in launching the containers.

For instance, if you don't need Chirpstack, you can start the `mosquitto` container first, 

```shell
docker compose -f mosquitto.yml up -d --build
```
and then start the `monapps` bundle by executing

```shell
docker compose -f monapps-dev.yml up -d --build
```
If you need only Chirpstack, then launch the `mosquitto.yml` file first, and then the `chirpstack.yml` file.

Later, you can start and stop the containers with the commands 

```shell
docker compose -f <file_name>.yml start
docker compose -f <file_name>.yml stop
```


## Usage
* When all the containers are running, find the web application at `your_host_ip:5000`. It will bring you to the admin panel. Log in to it using the credentials you previously put in the `.env` file.
* The database will be prepopulated with some items like ***datatypes*** and ***measurement units***. There will also be a couple of ***assets*** to see the ***status***/***current state*** propagation in action. To see applications in action, there is the application `SV leak detection by two temps`, four ***datafeeds***, and the ***task*** `App 1 Task` attached to the application. The source of data for the application is the device `Diagn kit 1` with two ***datastreams***, `temp1` and `temp2`. There is also the ***application*** `Stall/block detection by two temps`, which can also be used - just assign all ***datafeeds*** and the ***task*** to it, or create new ***datastreams*** and ***datafeeds***. All these items are disabled; you need to enable them by changing the `Is enabled` checkbox in the admin. 
* Read about dealing with the Chirpstack instance, connecting a LoRaWAN gateway with LoRa Packet Forwarder and delivering data from wireless devices to the Monapps instance in the [documentation](https://1drv.ms/f/c/d5a450febe052abb/IgDNEYUMorrNRIFrMlx0FIVJAdzSZM0VHofwtdzYhCB0kyc?e=wiuUZb).
* After onboarding a wireless diagnostic kit with two temperature sensors in Chirpstack, write down its `DEV EUI`, which will be used in `Monapps`. Go to the `monapps` admin panel, find in the "Devices" section the device `Device 1 Diagn kit 1` and replace the string in the input `dev ui` with the `DEV EUI`noted previously; then save.
* Go to the application that you are going to use and set up the `cursor ts` field. Use a UNIX timestamp in ms, it should be very close to the current moment. Use JS and `console.log((new Date()).getTime());`.  
* Now that the connection between the diagnostic kit and Monnaps is established, you can enable the items one by one. First, enable datastreams (so that the ***health*** is evaluated). Then enable the task. When the task is enabled, the ***health*** of the application is evaluated even if the application itself is off. And lastly, enable the application. It will start evaluating, and the values of ***status***/***current state***  will change after a certain time.
* You can see the results of the evaluation by hitting the API, for instance, `your_host_IP:5000/api/dfreadings/1/`. Also, it is possible to use the frontend app (read about it in the `Auxiliary repositories` section) for visualising the results.

## Auxiliary repositories

There are a couple of auxiliary repositories that may come in handy when getting familiar with the project:
* https://github.com/DmytroIE/MonappsSimulator It is a simulator that helps develop ***application functions***. Can provide a good graphical representation of the charts for ~100-150 ***resampling grid counts***, which is enough to test the basic logic. It repeats the code related to ***application functions*** execution in the original Monapps instance.
* https://github.com/DmytroIE/MonappsFront It is a frontend app for representing the results of both ***application functions*** and ***update functions***. It also comprises an MQTT client subscribed to the `procdata` topic, so all the changes in the ***health***, ***current state***, and ***status*** are reflected in the app immediately. It has never been tested with huge volumes of data, but for most cases it works fine.
* https://github.com/DmytroIE/MonappsStrata A small application that is also subscribed to the `procdata` topic but is interested only in new ***datafeed readings***. When new readings are created, it fetches them via an HTTP request, repacks, and sends them to Strata.