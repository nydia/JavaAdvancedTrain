@echo off
chcp 60001
:: 杀死所有 RocketMQ Java 进程
taskkill /f /fi "commandline like *rocketmq*" /im java.exe >nul 2>&1
:: 杀死所有带 RocketMQ 标题的窗口
taskkill /f /fi "windowtitle eq *RocketMQ*" /im cmd.exe >nul 2>&1
echo 已全部关闭
pause