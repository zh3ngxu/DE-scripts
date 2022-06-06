# Nginx docker demo

run the ngonix docker 

```
docker run -d --rm --name nginx01 -p 80:80 nginx:mainline-alpine
```

- docker run : start a container.
- -d : run in the background.
- --rm : remove the container when it exits.
- --name : the name of the container
- -p 80:80 : Map port 80 of the container to port 80 of the docker host
- nginx:mainline-alpine : The docker image to use


```
docker stop nginx01
```

index.html
```html
<html>
<head></head>
<body>
<div  align='left'><img src='https://s3.amazonaws.com/weclouddata/images/logos/wcd_logo_new_2.png' width='15%'></div >
<p style="font-size:30px;text-align:left"><b>GCP account setup</b></p>
<p style="font-size:20px;text-align:left"><b><font color='#F39A54'>Data Engineering Diploma</font></b></p>
<p style="font-size:15px;text-align:left">Content developed by: WeCloudData Academy</p>
<br>
<h1>My Test</h1>
<p>Custom html code!</p>
</body>
</html>
```

```
docker run -d -v ./custom_html/:/usr/share/nginx/html --rm --name nginx01 -p 80:80 nginx:mainline-alpine
```