# Monapps V5

The augmentation, restoration and catch-up algorithms are improved.

## Setup
* Download the project from Github
* Rename the `.example` files in the folder `docker/dev/env`  by removing `.example` from their names.
* Replace all the variables in `<>` with your own values. When generating a Django `SECRET_KEY` use the set of commands 
```shell
python manage.py shell
from django.core.management.utils import get_random_secret_key
print(get_random_secret_key())
```
You can alternatively use online resources like `https://djecrety.ir/` or `https://django-secret-key-generator.netlify.app/`.

## Start 
* Start Docker compose for the first time with the flag `--build`

```shell
docker compose up -d --build
```
To stop the bundle, execute

```shell
docker compose down
```

Later, you can start and stop the containers with the commands 

```shell
docker compose -f <file_name>.yml start
docker compose -f <file_name>.yml stop
```
If you don't need the Chirpstack, you can start the `mosquitto` container first, 

```shell
docker compose -f mosquitto.yml up -d --build
```
and then start the `monapps` bundle by executing

```shell
docker compose -f monapps-dev.yml up -d --build
```

In this case, you can post new datastrean readings by using the terminal in the `mosquitto` container. The command could look like this:

```shell
mosquitto_pub -t "rawdata" -m '{"0123456789abcdef": {"1759079024966": {"temp1": {"v": 110.1}, "temp2": {"v": 108.6}}, "1759079493999": {"temp1": {"v": 123.3}, "temp2": {"v": 120.8}}}}'
```

`NOTE`: If you need to make manipulations in the database, you can use the ORM and the command
```shell
python manage.py shell
``` 
or any other command that can be found in the Django documentation. Use the `monapps-beat` container for this purpose. In Docker Desktop chose `Open in terminal` and execute the command there.

* When all the containers are running, log in to the web application at `your_host_ip:5000`. It will bring you to the admin panel. Log in to it using the credentials you previously put in the .env file.
* The database will be prepopulated with some items. There will be a couple of assets to see the status/current state propagation in action. Additionally, there is the application `SV leak detection by two temps`, four datafeeds, and the task `App 1 Task` attached to the application. The source of data for the application is the device `Diagn kit 1` with two datastreams, `temp1` and `temp2`. There is also the application `Stall/block detection by two temps`, which can also be used - just assign all datafeeds and the task to it. All these items are disabled; you need to enable them by changing the `Is enabled` checkbox in the admin. But first, it is necessary to connect wireless devices to `Chirpstack`.
* Go to `your_host_ip:8080` and log in to Chirpstack. Provided that a LoRaWAN gateway with LoRa Packet Forwarder is connected to the PC that runs Docker, find the ID of this Packet Forwarder instance. In Chirpstack, open the `Gateways` tab and create a new gateway with the same ID. If everything is done correctly, in a couple of minutes, it should become **online** (green) in Chirpstack.
* Then add a `Device profile`. It would require, among other things, adding a proper uplink codec. In this bundle, you can find an uplink codec for “Enless diagnostic kits” in the folder `chirpstack_codecs` (the downlink codec is not finished and, at the moment, is represented as a stub in this file). Copy the content of this file, choose `Custom JS codec` in Chirpstack, and paste all this code there. Then, create an `Application` and create a diagnostic kit item there. Remember its `DEV EUI`, it will be used in `Monapps`.
* In `Monapps`, replace the string in the input `dev ui` of `Device 1 Diagn kit 1` with this `DEV EUI`and then save.
* Go to the application that you are going to use and set up the `cursor ts` field. Use a UNIX timestamp in ms, it should be very close to the current moment. Use JS and `console.log((new Date()).getTime());`.  
* Now that the connection between the diagnostic kit and Monnaps is established, you can enable the items one by one. First, enable datastreams (so that the **health** is evaluated). Then enable the task. When the task is enabled, then the **health** of the application is evaluated even if the application itself is off. And lastly, enable the application. It will start evaluating, and the values of **status**/**current** state will change after a certain time.
* You can see the results of the evaluation by hitting the API, for instance `your_host_IP:5000/api/dfreadings/1/`. Also, it is possible to use the frontend app (at the moment it works with a limited amount of datafeed readings).