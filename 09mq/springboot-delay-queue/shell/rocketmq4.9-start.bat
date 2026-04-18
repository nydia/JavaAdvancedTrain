@echo off
chcp 65001
setlocal enabledelayedexpansion

echo ==============================================
echo           RocketMQ 4.9.8 启动脚本 [已修复]
echo ==============================================

:: 你的路径（已修复语法）
set "JAVA_HOME=D:\soft\OpenJDK8U-jdk_x64_windows_hotspot_8u482b08\jdk8u482-b08"
set "ROCKETMQ_HOME=D:\soft\rocketmq-all-4.9.8-bin-release"
set "BROKER_CONF=!ROCKETMQ_HOME!\conf\standalone\broker-a.properties"

:: 修复环境变量
set "PATH=!JAVA_HOME!\bin;%PATH%"
set "CLASSPATH=!ROCKETMQ_HOME!\lib\*"

echo JDK: !JAVA_HOME!
echo RocketMQ: !ROCKETMQ_HOME!
echo 配置文件: !BROKER_CONF!
echo.

:: 启动 NameServer（修复命令语法）
echo [1/2] 启动 NameServer...
start "NameServer" cmd /k ""!ROCKETMQ_HOME!\bin\mqnamesrv.cmd""

:: 等待
timeout /t 3 /nobreak >nul

:: 启动 Broker（修复路径语法，核心解决报错）
echo [2/2] 启动 Broker...
start "Broker" cmd /k ""!ROCKETMQ_HOME!\bin\mqbroker.cmd" -n localhost:9876 -c "!BROKER_CONF!""

echo.
echo ================= 启动完成 ==================
echo 两个窗口已打开，不要关闭
echo ==============================================
pause