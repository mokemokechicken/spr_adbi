# 配備先と利用側との境界

確認日: 2026-10-24

## 何を配備するか

このrepositoryは`spr_adbi` Pythonライブラリを提供します。単独のservice process、runtime host、container image、queue、storage location、deployment workflowは定義しません。このライブラリを使うアプリケーションが配備先を所有します。このrepositoryのdependencyを更新しただけではconsumerの実行環境は変わりません。consumer側で新しいsource/versionをpinし、アプリ成果物をbuildして、そのrepository固有のdelivery手順でrolloutする必要があります。

| Component | Source entry point | 責務 |
| --- | --- | --- |
| Request client | `spr_adbi/client/adbi_client.py` (`ADBIClient.request`) | consumer設定のI/O clientでrequest inputを書き、function IDとrequest base URIを含むqueue messageを送る。 |
| Dispatcher | `spr_adbi/dispatcher/adbi_dispatcher.py` (`ADBIDispatcher`) | queue messageを受け、functionをresolveし、request statusを更新してmanagerへ処理を渡す。 |
| Worker manager | `spr_adbi/dispatcher/worker_manager.py` (`WorkerManager`) | run status/timestampを記録し、container managerを呼び出す。 |
| Container adapter | `spr_adbi/dispatcher/container.py` (`AWSContainerManager`) | consumer側のDocker daemonとregistry認証を使い、worker resolverから受け取ったimage URIをpullしてcontainerを起動する。 |
| Worker-side I/O | `spr_adbi/worker/adbi_worker.py` (`ADBIWorker`) | consumer workerにrequest arguments/input、status、output、progressのinterfaceを提供する。 |

Clientとdispatcherは、呼び出し元が渡すenvironment/optionsからqueueとI/O設定を読みます（`ADBI_SQS_NAME`、`ADBI_BASE_DIR`、`AWS_REGION`など）。これらはconsumer固有のenvironmentを指定する値であり、このpublic libraryのdefaultや配備先ではありません。

## Consumerを更新するとき

1. Consumer側のdependency referenceをreview済みのversionまたはcommitへ更新し、そのrepositoryの通常手順でlockfileを更新します。
2. Consumer側のtestとimage buildを実行します。
3. Consumerの文書化されたworkflowから成果物をpublishして配備します。対象はworker image、常駐dispatcher、別のadapterなどで、このlibraryが選択するものではありません。
4. Consumer側のread-only health/deployment確認で、runtime versionと動作を検証します。

このlibraryのclass名やexampleから実配備先を推定しないでください。example workerは開発用で、production consumerを特定する情報ではありません。Queue、I/O backend、registry access、resolver、runtime host、rollout procedureはconsumerの設定と文書で確認します。

## Sourceから確認できること

特定のcloud accountへ接続せず、sourceとinterfaceを読み取って確認します。

- `setup.py`: 配布packageと依存関係のmetadata
- `spr_adbi/const.py`: request/status pathと汎用environment variable名
- `spr_adbi/client/adbi_client.py`: request inputの書き込みとqueueへの送信
- `spr_adbi/dispatcher/adbi_dispatcher.py`、`spr_adbi/dispatcher/worker_manager.py`、`spr_adbi/dispatcher/container.py`: consumerがworkerをresolveして起動する流れ
- `spr_adbi/worker/adbi_worker.py`: worker側request/result interface

このrepositoryはconsumerのAWS account、host、queue名、storage bucket、catalog URL、image URI、rollout状態を示しません。それらは所有consumerのdocumentationとconfigurationで確認してください。このpublic guideには会社固有のinfrastructure identifierやinternal linkを含めていません。
