import pandas as ps

def regionName(df):
    df["Region"] = df["Region"].replace({"Tarapaca": "Tarapacá", "Valparaiso": "Valparaíso",
                                         "Del Libertador General Bernardo O’Higgins": "O’Higgins",
                                         "Ohiggins": "O’Higgins", "Nuble": "Ñuble",
                                         "Biobio": "Biobío", "La Araucania": "Araucanía", "Los Rios": "Los Ríos",
                                         "Aysen": "Aysén", "Magallanes y la Antartica": "Magallanes"
                                         })

def tableIdentifier(table):
    # la unica forma confiable de identificar las tablas es en base a sus columnas.
