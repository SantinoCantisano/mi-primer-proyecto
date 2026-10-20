# Entrega práctica 5 — MLOps con MLflow

- **Nombre:** Santino Cantisano
- **student_id:** santino_cantisano
- **Escala:** small

## Mis runs

Mis runs de la sección 1 del notebook 02 (P1–P10), con los valores de la tabla del notebook 03:

| run | modelo | features | class_weight | umbral | hiperparametros | train_pr_auc | pr_auc | accuracy | precision | recall | f1 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| P1 | logistic_regression | solo_regla | ninguno | 0.5 | C=1.0 | 0.179 | 0.170 | 0.896 | 0.0 | 0.0 | 0.0 |
| P2 | logistic_regression | basicas | ninguno | 0.5 | C=1.0 | 0.341 | 0.339 | 0.896 | 0.493 | 0.045 | 0.082 |
| P3 | logistic_regression | cliente | ninguno | 0.5 | C=1.0 | 0.447 | 0.448 | 0.902 | 0.614 | 0.157 | 0.250 |
| P4 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.531 | 0.545 | 0.917 | 0.758 | 0.288 | 0.417 |
| P5 | random_forest | completas | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.914 | 0.799 | 0.226 | 0.352 |
| P6 | gradient_boosting | completas | ninguno | 0.5 | learning_rate=0.1, max_depth=8 | 0.660 | 0.581 | 0.917 | 0.696 | 0.362 | 0.476 |
| P7 | random_forest | completas | balanced | 0.5 | n_estimators=100, max_depth=8 | 0.629 | 0.550 | 0.809 | 0.327 | 0.793 | 0.463 |
| P8 | random_forest | completas | ninguno | 0.3 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.912 | 0.592 | 0.486 | 0.534 |
| P9 | random_forest | completas | ninguno | 0.2 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.874 | 0.432 | 0.680 | 0.528 |
| P10 | gradient_boosting | completas | ninguno | 0.5 | learning_rate=0.1, max_depth=3 | 0.608 | 0.585 | 0.918 | 0.717 | 0.353 | 0.473 |

Barrido de `max_depth` (sección 3), random_forest con `completas`, class_weight=ninguno, umbral 0.5 y n_estimators=100:

| max_depth | train_pr_auc | pr_auc | accuracy | precision | recall | f1 |
|---|---|---|---|---|---|---|
| 3 | 0.535 | 0.525 | 0.896 | 0.0 | 0.0 | 0.0 |
| 5 | 0.561 | 0.541 | 0.903 | 0.906 | 0.078 | 0.144 |
| 8 | 0.643 | 0.568 | 0.914 | 0.799 | 0.226 | 0.352 |
| sin_limite | 1.000 | 0.537 | 0.916 | 0.689 | 0.341 | 0.456 |

La tabla completa del notebook 03 tiene muchos duplicados porque corrí varios barridos más de una vez y se acumulan en el experimento, dejo solo los runs que usé para responder.

**Run registrado:** `max_depth=3`: gradient_boosting, completas, class_weight=balanced, learning_rate=0.1, max_depth=3, umbral 0.5, PR-AUC 0.586. Salió de un barrido de `max_depth` con gradient boosting. Quedó como `workspace.bigdata_santino_cantisano.fraud_model`, versión 2, alias `champion`.

## Datos y tracking

**1.** Train tiene 35.654 filas (1 al 5 de marzo), test 7.129 (6 de marzo) y nuevas 7.165 (7 de marzo). Los contracargos son 11,05% en train, 10,38% en test (740 casos) y 11,05% en nuevas. Se separa por fecha porque en producción el modelo siempre predice el futuro con datos del pasado. Un split al azar mezclaría días y daría métricas optimistas que no se sostienen en la realidad.

**2.** Decir siempre "no hay contracargo" da una accuracy de 0,896 sin detectar un solo fraude. Con clases desbalanceadas (~10% de positivos) la accuracy premia no alertar. El PR-AUC, en cambio, mide qué tan bien el modelo ordena el riesgo sobre la clase positiva, y su referencia al azar es apenas ~0,104.

**3.** En `01_manual_logistica` quedaron como parámetros `model_type`=logistic_regression, `feature_set`=basicas, C=1.0, `class_weight`=ninguno, `threshold`=0.5, `data_table`=ml_features y `data_version`=1. Como métricas, test_pr_auc 0,339, test_roc_auc 0,794, test_accuracy 0,896, test_precision 0,493, test_recall 0,045, test_f1 0,082, test_alertas 67 y train_pr_auc 0,341. Como artefacto quedó `matriz_confusion.png`, y el modelo con su firma aparece aparte en *Logged models* porque es MLflow 3. Un experimento es la carpeta que agrupa todos los intentos sobre un mismo problema, y un run es una ejecución puntual de entrenamiento dentro de él.

**4.** Autolog registró 46 parámetros: todos los del Pipeline, incluso los default, como `modelo__criterion` o `modelo__max_features`. Las métricas fueron solo de entrenamiento: training_accuracy_score 0,91, training_f1_score 0,887, training_log_loss 0,229, training_precision_score 0,901, training_recall_score 0,91, training_roc_auc 0,899 y training_score 0,91. Lo que no registra es lo propio del problema, las métricas sobre test, el umbral de negocio, el conjunto de features y la versión de los datos.

**5.** Sí, `01_repro_a` y `01_repro_b` dieron exactamente lo mismo (PR-AUC 0,5099, recall 0,2068, 212 alertas) y la comparación dio `True`. Para que un run sea reproducible hay que fijar los datos (`data_version`, la versión Delta de `ml_features`), el código, la semilla (`random_state=42`) y el entorno (el `requirements.txt` que MLflow guarda junto al modelo).

**6.** Al llamar al modelo sin la columna `amount`, MLflow cortó antes de predecir con `Failed to enforce schema of data`, porque la firma declara `amount: double (required)` y `payment_channel: string (required)`. La firma (*signature*) documenta qué columnas y tipos espera el modelo como entrada y qué devuelve. Así se validan los datos antes de predecir y se evita que falle en silencio o prediga basura.

## Tus experimentos

**7.** Mi mejor run fue gradient boosting con `completas`, class_weight=balanced, learning_rate=0,1, max_depth=3 y umbral 0,5, con un PR-AUC de 0,586. Contra `solo_regla` (PR-AUC 0,170) es una mejora de +0,416, unas 3,4 veces más.

**8.** Con la logística, el PR-AUC pasó de 0,339 (`basicas`) a 0,448 (`cliente`) y a 0,545 (`completas`), así que cada grupo de variables suma bastante. En el mejor run las más importantes fueron `amount` (0,327), `payment_channel` (0,175), `hour` (0,103), `category` (0,074) y `account_age_days` (0,059).

**9.** Con `completas` anduvo mejor el boosting (0,581 con depth 8 y 0,585 con depth 3), después el bosque (0,568) y último la logística (0,545). Un árbol parte los datos en reglas encadenadas (por ejemplo "si es de noche *y* paga por transferencia, entonces…"), así que captura interacciones solo. La logística suma el efecto de cada variable por separado, y para ver esa combinación habría que crear la interacción a mano como feature.

**10.** En el bosque con `completas`, pasar a `balanced` bajó la precision de 0,799 a 0,327, subió el recall de 0,226 a 0,793 y las alertas pasaron de 209 a ~1.800. El PR-AUC casi no cambió (0,568 → 0,550; en boosting 0,585 → 0,586). Balancear mueve el punto de trabajo hacia alertar más, pero no hace que el modelo ordene mejor.

**11.** En el bosque, bajar el umbral de 0,5 a 0,3 y a 0,2 subió el recall (0,226 → 0,486 → 0,680), bajó la precision (0,799 → 0,592 → 0,432) y las alertas pasaron de 209 a ~600 y ~1.150. El F1 cambió (0,352 → 0,534 → 0,528) pero el PR-AUC quedó clavado en 0,568. El PR-AUC se calcula recorriendo todos los umbrales posibles sobre los scores, mientras que el F1 sale de las predicciones binarias a un umbral puntual.

**12.** Barrí `max_depth` en un random forest con `completas`. El mejor PR-AUC de test salió con max_depth=8 (0,568). Con `sin_limite` el train_pr_auc llegó a 1,000 mientras el de test bajó a 0,537. Eso es sobreajuste: el modelo memoriza el entrenamiento en lugar de aprender patrones que generalicen. En el boosting se vio lo mismo, más suave: train de 0,608 a 0,674 con el test plano en ~0,58.

## Comparar y elegir

**13.** No coincidieron. El mejor por PR-AUC fue el boosting `balanced` (PR-AUC 0,586, accuracy 0,809) y el mejor por accuracy fue P10, el mismo boosting sin balancear (accuracy 0,918, PR-AUC 0,585). La accuracy favorece al modelo que alerta poco, porque con ~90% de negativos acertar los "no" ya da buen número. El balanceado genera muchas alertas falsas y eso le baja la accuracy, aunque ordene el riesgo igual o mejor.

**14.** Los que más se alejan de la diagonal son los random forest con `max_depth=sin_limite` (train 1,000 vs test ~0,535), y en menor medida los boosting con depth 5, 8 y sin límite (~0,67 vs ~0,58). Eso indica sobreajuste. Las logísticas quedan pegadas a la diagonal (0,531 vs 0,545), así que generalizan bien pero son menos potentes.

**15.** Con `search_runs` los runs quedan como un DataFrame que se puede filtrar, ordenar y comparar con código. Es reproducible y escala a cientos de runs, cosa que mirando la UI a mano no pasa. El filtro que usaría:

```python
filter_string = "tags.student_id = 'santino_cantisano' AND params.model_type = 'random_forest' AND metrics.test_pr_auc > 0.55"
```

**16.** En general cuesta más un contracargo no detectado: se pierde la plata más el costo de la disputa, mientras que una alerta falsa es tiempo de analista y algo de fricción con el cliente. Si el equipo puede revisar una cantidad fija por día, elegiría el umbral para que las alertas esperadas igualen esa capacidad, o sea, revisar el top-K por score. El modelo ordena y el umbral se ajusta a la operación.

## Registro, uso y monitoreo

**17.** Sí, quedó registrado como `workspace.bigdata_santino_cantisano.fraud_model`, versión 2, con alias `champion`. Hay dos versiones porque corrí el registro dos veces sobre el mismo run. Cargar `models:/...@champion` desacopla el proceso que consume el modelo de qué modelo es: para cambiarlo se mueve el alias a otra versión sin tocar el código. Con `runs:/<run_id>/model` habría que cambiar el ID a mano cada vez.

**18.** Se puntuaron 7.165 transacciones nuevas y 1.825 generaron alerta (25,5%). Para auditar cada decisión están `transaction_id`, `score`, `alerta`, `threshold`, `model_name`, `model_version`, `run_id` y `scored_at`. El umbral se guarda fuera del modelo porque es una decisión de negocio, por ejemplo según la capacidad del equipo de riesgo, y se tiene que poder cambiar sin reentrenar.

**19.** Sí, quedó muy parecido: PR-AUC 0,579 en nuevas contra 0,586 en test, con precision y recall casi iguales. En la campaña simulada el porcentaje de alertas saltó de 25,5% a 42,9%. Drift es cuando cambia la distribución de los datos de entrada (o la relación con la etiqueta) respecto de lo que vio el modelo al entrenar. Se puede detectar antes de tener etiquetas porque alcanza con comparar las distribuciones de las features y de los scores, como importe promedio, % de transferencias o % de alertas.

## Cierre

**20.** En el 00 hice la parte de datos (armar `ml_features` con split temporal). En el 01 y el 02, entrenamiento y tracking, con cada run registrado en MLflow. En el 03, comparación y selección. En el 04, registro en Unity Catalog con alias, despliegue como inferencia batch a `ml_predictions` y monitoreo con detección de drift. Para tener esto en producción real faltaría, como mínimo:
- un job programado que puntúe automáticamente las transacciones nuevas;
- monitoreo automático con alertas cuando el drift o las métricas pasen un umbral;