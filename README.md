# Analizador de pdf de MINSAL basado en AWS Textract

El objetivo de este proyecto es procesar rapidamente la información provista por MINSAL en pdfs.

El flujo de procesamiento es:

 1. Bajamos pdf publicados por MINSAL, especificamente 3:
    * Reporte diario, de https://www.gob.cl/coronavirus/cifrasoficiales/
    * Informe Epidemiologico, de https://www.gob.cl/coronavirus/cifrasoficiales/
    * Informe de Situacion COVID19, de http://epi.minsal.cl/informes-covid-19/
 2. Estos archivos se guardan localmente en `input`
 3. A continuación se suben a un bucket privado en AWS, `do-covid19`
 4. Esos pdfs son procesados por `textract`
 5. El output es rescatado, transformado a pandas dataframe y escrito a disco como pdf.
 6. Post procesamos (POR HACER!)
 
 Usamos el disco local como buffer para cada etapa dado que el proyecto se encuentra en desarrollo.
 Por eso el repo es pesado y lleva pdfs y csvs ya recuperados.
 
 Si quieres correr el script, se genera un archivo `jobs.log` que tiene los ids de los jobs ejecutados.
 Con esos ids puedes invocar el `process.py` con el flag `test=True`, obteniendo los resultados de textract rapidamente
 
 ## Por hacer
 
 Necesitamos procesar los csv que entrega amazon para que puedan ser consumidos por los scripts que generan los
 productos en el repo del MINCIENCIA