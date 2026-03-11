# NeuG Java Driver 使用指南

## 在其他项目中使用

### 方式一：本地 Maven 仓库（开发测试）

1. 安装到本地 Maven 仓库：
```bash
cd /data/0319/neug2/tools/java
mvn clean install -DskipTests
```

2. 在其他项目的 `pom.xml` 中添加依赖：
```xml
<dependency>
    <groupId>org.alibaba.neug</groupId>
    <artifactId>neug-java-driver</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 方式二：使用本地 JAR 文件

如果不想使用 Maven 仓库，可以直接引用编译好的 JAR：

```xml
<dependency>
    <groupId>org.alibaba.neug</groupId>
    <artifactId>neug-java-driver</artifactId>
    <version>1.0.0-SNAPSHOT</version>
    <scope>system</scope>
    <systemPath>/data/0319/neug2/tools/java/target/neug-java-driver-1.0.0-SNAPSHOT.jar</systemPath>
</dependency>
```

### 方式三：发布到私有 Maven 仓库（生产环境推荐）

1. 在 `pom.xml` 中添加 distributionManagement 配置：
```xml
<distributionManagement>
    <repository>
        <id>your-releases</id>
        <name>Your Release Repository</name>
        <url>http://your-nexus-server/repository/maven-releases/</url>
    </repository>
    <snapshotRepository>
        <id>your-snapshots</id>
        <name>Your Snapshot Repository</name>
        <url>http://your-nexus-server/repository/maven-snapshots/</url>
    </snapshotRepository>
</distributionManagement>
```

2. 在 `~/.m2/settings.xml` 中配置仓库认证信息：
```xml
<servers>
    <server>
        <id>your-releases</id>
        <username>your-username</username>
        <password>your-password</password>
    </server>
    <server>
        <id>your-snapshots</id>
        <username>your-username</username>
        <password>your-password</password>
    </server>
</servers>
```

3. 发布到仓库：
```bash
mvn clean deploy
```

## 使用示例

### 基本连接

```java
import org.alibaba.neug.driver.*;

public class Example {
    public static void main(String[] args) {
        // 创建驱动
        Driver driver = GraphDatabase.driver("http://localhost:8000");
        
        try {
            // 验证连接
            driver.verifyConnectivity();
            
            // 创建会话
            try (Session session = driver.session()) {
                // 执行查询
                try (ResultSet rs = session.run("MATCH (n) RETURN n LIMIT 10")) {
                    while (rs.next()) {
                        System.out.println(rs.getObject("n"));
                    }
                }
            }
        } finally {
            driver.close();
        }
    }
}
```

### 带配置的连接

```java
import org.alibaba.neug.driver.*;
import java.util.concurrent.TimeUnit;

public class ConfigExample {
    public static void main(String[] args) {
        Config config = Config.builder()
            .withConnectionTimeout(30, TimeUnit.SECONDS)
            .withMaxRetries(3)
            .build();
        
        Driver driver = GraphDatabase.driver("http://localhost:8000", config);
        
        try (Session session = driver.session(AccessMode.READ)) {
            // 只读查询
            try (ResultSet rs = session.run("MATCH (n:Person) RETURN n.name, n.age")) {
                while (rs.next()) {
                    String name = rs.getString("n.name");
                    int age = rs.getInt("n.age");
                    System.out.println(name + ", " + age);
                }
            }
        } finally {
            driver.close();
        }
    }
}
```

### 带参数的查询

```java
import java.util.HashMap;
import java.util.Map;

Map<String, Object> parameters = new HashMap<>();
parameters.put("name", "Alice");
parameters.put("age", 30);

try (Session session = driver.session()) {
    String query = "CREATE (p:Person {name: $name, age: $age}) RETURN p";
    try (ResultSet rs = session.run(query, parameters)) {
        if (rs.next()) {
            System.out.println("Created: " + rs.getObject("p"));
        }
    }
}
```

## Gradle 项目使用

如果使用 Gradle，在 `build.gradle` 中添加：

```groovy
dependencies {
    implementation 'org.alibaba.neug:neug-java-driver:1.0.0-SNAPSHOT'
}
```

对于本地 JAR：
```groovy
dependencies {
    implementation files('/data/0319/neug2/tools/java/target/neug-java-driver-1.0.0-SNAPSHOT.jar')
    implementation 'com.squareup.okhttp3:okhttp:4.11.0'
    implementation 'com.google.protobuf:protobuf-java:4.29.6'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
}
```

## 依赖说明

该 driver 依赖以下库：
- OkHttp 4.11.0 - HTTP 客户端
- Protocol Buffers 4.29.6 - 序列化
- Jackson 2.15.2 - JSON 处理
- SLF4J 2.0.7 - 日志接口

这些依赖会被 Maven 自动管理。
