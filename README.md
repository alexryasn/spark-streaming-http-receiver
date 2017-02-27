This simple apps propose custom Apache Spark Streaming receiver, based on embedded Apache Tomcat.
So it transfers http requests params to Apache Spark Streaming structure

How to use:
- run application
- call `curl "http://localhost:8080/event?type=START&device=mobile"` (you can use any params instead of `type` and `device`)
- you`ll see in console something like this:

```
-------------------------------------------
Time: 1487605815000 ms
-------------------------------------------
{"type":["START"],"device":["mobile"],"timestamp":["1487605811615"]}
{"type":["START"],"device":["mobile"],"timestamp":["1487605813168"]}
```