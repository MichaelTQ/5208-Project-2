# 三人统一运行环境

环境由 A 统一，B/C 参照。核心版本为 Python 3.11.14、PySpark/Spark 3.5.8、Java 17。A 实际使用 Eclipse Temurin 17.0.20.1+1（macOS ARM64）。其他系统优先使用相同 Temurin 版本；若使用其他 Java 17 补丁版，须记录差异并重跑验证。

## A 的实际本机

- macOS 26.1，ARM64。
- 10 个物理核、10 个逻辑核，内存 16 GiB。
- Stage 1 开始时磁盘可用空间约 539 GiB。
- Spark 使用 local[4]，driver memory 4g，时区 Asia/Singapore。Stage 1 使用 16 个 shuffle 分区；Stage 2 全量处理固定为 64（config/stage2.json），减少每个分区处理的记录量。正式输出按年月分区，雨量采用 Decimal(20,9)。
- 本地单机模式；四条本地执行线程，未配置远程 worker/executor 节点。
- AQE 的实际状态、包版本、Java 输出及检查时磁盘余量见 artifacts/environment/environment.json。

这些是起始配置，后续全量清洗可根据实际运行调优，并记录修改。未使用云 VM，因此无需虚构云节点规格。

Stage 2 全量处理需要额外空间存放 parsed/grouped 检查点和磁盘 shuffle。最终交接不包含这些中间结果或 Spark 临时目录。各阶段完成时记录实际耗时；B/C 保持版本与时区一致，计算参数调整须另行记录。

## 安装与运行

先安装 uv（官方说明：https://docs.astral.sh/uv/getting-started/installation/）。在项目根目录运行：

```bash
bash scripts/setup.sh
bash scripts/run.sh scripts/verify_spark.py
```

macOS ARM64 的 setup.sh 下载 Java 官方归档，校验发布方 SHA-256，并解压到项目 .runtime；不会使用系统现有的 Java 16。Python 和依赖也保存在项目 .runtime 和 .venv 中。

Linux 或 macOS Intel 用户安装 Temurin Java 17 后，设置 JAVA_HOME 指向自己的 JDK，再用 scripts/run.sh。Windows 可在 WSL 使用上述脚本，或手动创建 Python 3.11.14 虚拟环境、安装 requirements.lock.txt，配置 JAVA_HOME 和 PYSPARK_PYTHON 后运行 Python 脚本。其他平台尚未在本机验证，不能把 macOS 验证当成所有系统均验证。

运行脚本的路径均相对项目位置解析。不要将 A 的 .venv 或 .runtime 直接复制给 B/C；他们根据锁定文件建立自己的环境。requirements.txt 是直接依赖，requirements.lock.txt 固定本次安装的全部 Python 包版本。

Stage 4 提供 `bash scripts/run.sh scripts/run_pipeline.py --mode preflight`：读取当前机器的 Python、全部锁定包和 Java，再实际启动 Spark，检查 SGT、四线程、4 GiB 堆及十进制计算。结果在 artifacts/stage4/preflight.json；旧 environment.json 不能代替 B/C 机器的当前检查。统一样本/复核/重建入口见 docs/RUN_GUIDE.md。

## 官方依据

- Spark 3.5.8 环境说明：https://spark.apache.org/docs/3.5.8/
- PySpark 3.5.8：https://pypi.org/project/pyspark/3.5.8/
- Python 3.11.14：https://www.python.org/downloads/release/python-31114/
- Eclipse Temurin：https://adoptium.net/installation

下载均使用 HTTPS 证书验证。下载脚本使用系统 curl，避免本机已有 Python 缺少 CA 路径时的证书错误；不会关闭 TLS 校验。
