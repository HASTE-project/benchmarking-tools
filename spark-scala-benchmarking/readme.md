To run:

```
/Library/Java/JavaVirtualMachines/jdk-9.0.1.jdk/Contents/Home/bin/java -Xms512M -Xmx1024M -Xss1M -XX:+CMSClassUnloadingEnabled "-javaagent:/Applications/IntelliJ IDEA CE.app/Contents/lib/idea_rt.jar=51418:/Applications/IntelliJ IDEA CE.app/Contents/bin" -Dfile.encoding=UTF-8 -classpath "/Users/benblamey/Library/Application Support/IdeaIC2017.3/Scala/launcher/sbt-launch.jar" xsbt.boot.Boot package
sh ./deploy.bash
```