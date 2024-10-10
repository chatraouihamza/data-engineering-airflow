from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime, timedelta
import csv
import time
import pendulum

# Définir les fonctions utilisées dans le DAG
def linkextraction():
  

    # Configuration du service pour ChromeDriver
    service = Service('/usr/local/bin/chromedriver')

    # Configuration des options de Chrome
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  # Exécuter en mode headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")
    # Initialisation du navigateur avec le service et les options
    browser = webdriver.Chrome(service=service, options=chrome_options)
    i = 1
    links = []
    start_time = time.time()

    try:
        while True:
            # Charger la page avec le numéro de page actuel
            url = f"https://www.avito.ma/fr/maroc/maisons-%C3%A0_vendre?o={i}"
            browser.get(url)
            
            try:
                # Attendre que le conteneur principal soit chargé
                main = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main')))
                
                # Trouver les éléments contenant les liens
                elements = main.find_elements(By.XPATH, '//*[@id="__next"]/div/main/div/div[5]/div[1]/div/div[1]//a')
                
                # Parcourir chaque élément <a> pour extraire l'attribut href
                for element in elements:
                    try:
                        href = element.get_attribute('href')
                        if href:
                            links.append(href)
                    except Exception as ex:
                        print(f"An error occurred while extracting link: {str(ex)}")
            
            except TimeoutException:
                print(f"Timeout exception occurred while loading page {i}")
            
            # Incrémenter le numéro de page pour charger la suivante
            i += 1
            # Vérifier si le temps écoulé dépasse 5 minutes (300 secondes)
            if time.time() - start_time > 60:
                print("Temps d'exécution de 5 minutes dépassé.")
                break
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Fermer le navigateur à la fin
        browser.quit()

        # Sauvegarder les URLs dans un fichier CSV avec UTF-8 encoding
        with open('urls.csv', 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['links'])
            for url in links:
                writer.writerow([url])

        print(f"The URLs have been saved to 'urls.csv' with UTF-8 encoding.")
            # Code pour l'extraction des liens
        print("Extraction des liens effectuée")

def ExtractionImmobiliers():   
    import csv
    import pandas as pd
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from bs4 import BeautifulSoup

    # Configuration du service pour ChromeDriver
    service = Service('/usr/local/bin/chromedriver')

    # Configuration des options de Chrome
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")  # Exécuter en mode headless
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-debugging-port=9222")
    # Initialisation du navigateur avec le service et les options
    browser = webdriver.Chrome(service=service, options=chrome_options)

    # Lecture des URLs depuis un fichier CSV
    df = pd.read_csv('urls.csv')

    # Noms de colonnes pour le fichier CSV final
    fieldnames = ['Description', 'Localisation', 'Prix', 'Surface Total', 'Nombre de Salles de Bain', 'Nombre de Chambres', 'Type', 'Secteur', 'Nombre d\'Étage', 'Âge du Bien', 'Surface Habitable', 'Nombre de Salons']
    
    # Ouvrir un fichier CSV pour écrire les données extraites
    with open('donnees_immobilieres.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
        # start2_time = time.time()
        # Parcourir chaque URL dans le DataFrame
        for index, row in df.iterrows():
            url = row['links']  # Assurez-vous que 'URL' est le nom de la colonne dans votre fichier CSV

            try:
                browser.get(url)
                main = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/main')))
                
                # Utilisation de XPath pour localiser chaque élément
                classe = main.find_element(By.XPATH,'/html/body/div[1]/div/main/div/div[3]/div[1]/div[2]/div[1]/div[1]/div[2]/div[1]')
                description = classe.find_element(By.XPATH,'/html/body/div[1]/div/main/div/div[3]/div[1]/div[2]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]')
                localisation = classe.find_element(By.XPATH,'/html/body/div[1]/div/main/div/div[3]/div[1]/div[2]/div[1]/div[1]/div[2]/div[1]/div[2]/span[1]')
                prix = classe.find_element(By.XPATH,'/html/body/div[1]/div/main/div/div[3]/div[1]/div[2]/div[1]/div[1]/div[2]/div[1]/div[1]/div[2]/p')
                element = WebDriverWait(browser, 10).until(EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div/main/div/div[3]/div[1]/div[2]/div[1]/div[1]/div[2]/div[4]')))
                
                # Utilisation de BeautifulSoup pour traiter le code HTML
                outer_elm = element.get_attribute('outerHTML')
                soup = BeautifulSoup(outer_elm, 'html.parser')
                
                divayat3 = soup.find_all('div', class_='sc-6p5md9-1 ksqQXN')
                result_dict = {}
                result_dict2 = {}

                # Parcourir chaque div pour extraire le titre et le texte du span
                for div in divayat3:
                    title = div.find('title').text.strip()
                    span_text = div.find('span', class_='sc-1x0vz2r-0 kQHNss').text.strip()
                    result_dict[title] = span_text

                l3ibat = soup.find_all('li', class_='sc-qmn92k-1 jJjeGO')
                for l3iba in l3ibat:
                    titre = l3iba.find_all('span')
                    result_dict2[titre[0].text.strip()] = titre[1].text.strip()

                # Fusionner les dictionnaires
                merged_dict = {
                    'Description': description.text,
                    'Localisation': localisation.text,
                    'Prix': prix.text,
                    'Surface Total': result_dict.get('SurfaceTotale Icon', ''),
                    'Nombre de Salles de Bain': result_dict.get('SalleDeBain Icon', ''),
                    'Nombre de Chambres': result_dict.get('Chambres Icon', ''),
                    'Type': result_dict2.get('Type', ''),
                    'Secteur': result_dict2.get('Secteur', ''),
                    'Nombre d\'Étage': result_dict2.get("Nombre d'étage", ''),
                    'Âge du Bien': result_dict2.get('Âge du bien', ''),
                    'Surface Habitable': result_dict2.get('Surface habitable', ''),
                    'Nombre de Salons': result_dict2.get('Salons', '')
                }

                # Écrire dans le fichier CSV
                writer.writerow(merged_dict)
                # if time.time() - start2_time > 400:
                #    print("Temps d'exécution de 6;40 minutes dépassé.")
                # break
            except Exception as e:
                print(f"Une erreur s'est produite pour l'URL {url}: {e}")

    # Fermer le navigateur à la fin
    browser.quit()
    # S'assurer que la tâche dure au moins 5 minutes
    # elapsed_time = time.time() - start2_time
    # if elapsed_time < 300:
    #     time.sleep(300 - elapsed_time)
    # # Code pour l'extraction des données immobilières
    print("Extraction des données immobilières effectuée")

def preparation():
    
    import pandas as pd
    import numpy as np

    df = pd.read_csv('donnees_immobilieres.csv')
    # df = pd.read_csv('C:\\Users\PC\\OneDrive\\Dokumente\\Data Engineering\\donnees_immobilieres.csv')
    # Sélectionner les colonnes numériques pour l'imputation
    numeric_columns = ['Nombre de Salles de Bain', 'Nombre de Chambres','Surface Habitable']
    # df['Nombre d\'Étage']=df['Nombre d\'Étage'].fillna(df['Nombre d\'Étage'].mean()).astype(int)
    # Supprimer les lignes où 'Nombre d'Étage' est nul
    df = df.dropna(subset=['Nombre d\'Étage'])
    df = df.dropna(subset=['Surface Total'])
    df = df.dropna(subset=['Nombre de Chambres'])
    df = df.dropna(subset=['Nombre de Salons'])
    df = df.dropna(subset=['Prix'])
    df = df.dropna(subset=['Surface Habitable'])
    df = df.dropna(subset=['Nombre de Salles de Bain'])

    # clean
    df['Nombre d\'Étage'] =pd.to_numeric(df['Nombre d\'Étage'].astype(str).str.replace(r'\D', '', regex=True), errors='coerce')
    df['Surface Total'] = pd.to_numeric(df['Surface Total'].astype(str).str.replace(r'\D', '', regex=True), errors='coerce')
    df['Nombre de Salons'] = pd.to_numeric(df['Nombre de Salons'].astype(str).str.replace(r'\D', '', regex=True), errors='coerce')
    # Supprimer les caractères "DH" et les espaces
    df = df[df['Prix'] != "PRIX NON SPÉCIFIÉ"]
    df['Prix'] = df['Prix'].str.replace('DH', '').str.replace(' ', '').str.replace('\u202f', '')

    # Nettoyage des colonnes
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'\D', '', regex=True), errors='coerce')
    # Convertir la colonne "Prix" en type numérique
    df['Prix'] = pd.to_numeric(df['Prix'], errors='coerce')
    # Transformer la colonne "Nombre de Salons" en type entier
    df['Nombre de Salons'] = df['Nombre de Salons'].astype(int)

    df.rename(columns={'Prix': 'Prix_en_DH'}, inplace=True)
    df.rename(columns={'Surface Total': 'Surface_Total'}, inplace=True)
   
    return df


def DashApp():
    import dash
    from dash import dcc
    from dash import html
    import plotly.express as px
    import plotly.graph_objects as go
    from scipy import stats
    import numpy as np
 
    # Charger le DataFrame depuis le CSV en utilisant la fonction preparation()
    df = preparation()

    # Filtrer les données avec NumPy
    df_filtered = df[(df['Surface_Total'] <= 3000) & (df['Prix_en_DH'] <= 5000000)]

    # Utiliser NumPy pour mettre à l'échelle le prix en DH
    df_filtered['Prix_en_DH_scaled'] = np.divide(df_filtered['Prix_en_DH'], 1000000)

    df_filtered = df_filtered.dropna()

    # Calculer la régression linéaire
    slope, intercept, r_value, p_value, std_err = stats.linregress(df_filtered['Surface_Total'], df_filtered['Prix_en_DH_scaled'])
    predicted_prices = slope * df_filtered['Surface_Total'] + intercept

    # Trier par prix moyen décroissant et sélectionner les 15 premières villes
    df_sorted = df_filtered.sort_values(by='Prix_en_DH_scaled', ascending=False).head(15)

    # Calculer la moyenne des surfaces
    Surface_Total_max = df['Surface_Total'].mean()
    Surface_hab_max = df['Surface Habitable'].mean()

    # Créer le scatter plot avec Plotly
    scatter_surface_plot = px.scatter(
        df,
        x='Surface_Total',
        y='Surface Habitable',
        title='Corrélation entre Surface Habitable et Total',
        labels={'Surface_Total': 'Surface Totale (m²)', 'Surface Habitable': 'Surface Habitable (m²)'},
        color_discrete_sequence=['red']
    )
    scatter_surface_plot.update_layout(
        xaxis=dict(range=[0, Surface_Total_max]),
        yaxis=dict(range=[0, Surface_hab_max]),
        title_text='Corrélation entre Surface Habitable et Total'
    )

    def create_box_plot_chambres(df, ville):
        df_filtered_ville = df[df['Localisation'] == ville]
        return {
            'data': [
                go.Box(
                    y=df_filtered_ville['Nombre de Chambres'],
                    name='Nombre de Chambres',
                    marker_color='blue'
                )
            ],
            'layout': {
                'title': f'Box Plot du Nombre de Chambres à {ville}',
                'yaxis': {'title': 'Nombre de Chambres'},
                'boxmode': 'group'
            }
        }

    def create_box_plot_surface(df, ville):
        df_filtered_ville = df[df['Localisation'] == ville]
        return {
            'data': [
                go.Box(
                    y=df_filtered_ville['Surface_Total'],
                    name='Surface Totale',
                    marker_color='red'
                )
            ],
            'layout': {
                'title': f'Box Plot de la Surface Totale à {ville}',
                'yaxis': {'title': 'Surface Totale en m²'},
                'boxmode': 'group'
            }
        }

    def create_box_plot_prix(df, ville):
        df_filtered_ville = df[df['Localisation'] == ville]
        return {
            'data': [
                go.Box(
                    y=df_filtered_ville['Prix_en_DH_scaled'],
                    name='Prix (scaled)',
                    marker_color='green'
                )
            ],
            'layout': {
                'title': f'Box Plot du Prix à {ville}',
                'yaxis': {'title': 'Prix en DH (divisé par 1 000 000 DHS)'},
                'boxmode': 'group'
            }
        }

    # Créer une application Dash
    app = dash.Dash(__name__)

    # Layout de l'application Dash
    app.layout = html.Div([
        html.H1("Dashboard Data Immobiliers"),

        # Graphiques existants
        dcc.Graph(
            id='scatter-plot',
            figure={
                'data': [
                    {
                        'x': df_filtered['Surface_Total'],
                        'y': df_filtered['Prix_en_DH_scaled'],
                        'mode': 'markers',
                        'marker': {
                            'size': 8,
                            'opacity': 0.7,
                            'color': 'blue'  # Couleur des points
                        },
                        'name': 'Données réelles'
                    },
                    {
                        'x': df_filtered['Surface_Total'],
                        'y': predicted_prices,
                        'mode': 'lines',
                        'line': {
                            'color': 'red',
                            'width': 3,
                            'dash': 'dash'  # Type de ligne en pointillés
                        },
                        'name': 'Ligne de régression linéaire'
                    }
                ],
                'layout': {
                    'title': 'Relation entre Surface Totale et Prix (Prix divisé par 1 000 000 DHS)',
                    'xaxis': {'title': 'Surface Totale en m²', 'range': [0, 3000]},
                    'yaxis': {'title': 'Prix en DH (divisé par 1 000 000 DHS)'},
                    'hovermode': 'closest'  # Mode d'affichage au survol
                }
            }
        ),

        dcc.Graph(
            id='bar-plot',
            figure={
                'data': [
                    {
                        'x': df_sorted['Localisation'],
                        'y': df_sorted['Prix_en_DH_scaled'],
                        'type': 'bar',
                        'marker': {'color': 'green'},  # Couleur des barres
                    }
                ],
                'layout': {
                    'title': 'Distribution des Prix par Localisation',
                    'xaxis': {'title': 'Localisation'},
                    'yaxis': {'title': 'Prix en DH (divisé par 1 000 000 DHS)'},
                    'barmode': 'group',  # Mode d'affichage des barres
                }
            }
        ),

        dcc.Graph(
            id='scatter-surface-plot',
            figure=scatter_surface_plot
        ),

        dcc.Dropdown(
            id='ville-dropdown',
            options=[{'label': ville, 'value': ville} for ville in df['Localisation'].unique()],
            value=df['Localisation'].unique()[0],  # Valeur par défaut
            style={
                'width': '95%',  # Largeur du menu déroulant
                'padding': '5px',  # Espacement interne
                'backgroundColor': 'yellow',  # Couleur de fond
                'border': '1px solid #ccc',  # Bordure
                'borderRadius': '5px',  # Coins arrondis
            },
            clearable=True  # Ajouter une option pour effacer la sélection
        ),

        # Graphiques pour la sélection des villes
        dcc.Graph(id='scatter-plot-ville'),
        dcc.Graph(id='bar-plot-ville'),
        dcc.Graph(id='pie-chart-ville'),
        dcc.Graph(id='box-plot-chambres'),
        dcc.Graph(id='box-plot-surface'),
        dcc.Graph(id='box-plot-prix')
    ])

    # Définir le callback pour mettre à jour les graphiques en fonction de la ville sélectionnée
    @app.callback(
        [dash.dependencies.Output('scatter-plot-ville', 'figure'),
        dash.dependencies.Output('bar-plot-ville', 'figure'),
        dash.dependencies.Output('pie-chart-ville', 'figure'),
        dash.dependencies.Output('box-plot-chambres', 'figure'),
        dash.dependencies.Output('box-plot-surface', 'figure'),
        dash.dependencies.Output('box-plot-prix', 'figure')],
        [dash.dependencies.Input('ville-dropdown', 'value')]
    )
    def update_graphs_ville(ville):
        # Filtrer les données pour la ville sélectionnée
        df_filtered_ville = df[df['Localisation'] == ville]

        # Vérifier si df_filtered_ville est vide
        if df_filtered_ville.empty:
            return {}, {}, {}, {}, {}, {}

        # Ajouter l'échelle des prix en DH pour les données filtrées
        df_filtered_ville['Prix_en_DH_scaled'] = np.divide(df_filtered_ville['Prix_en_DH'], 1000000)

        # Graphique de dispersion (scatter plot)
        scatter_fig_ville = {
            'data': [
                {
                    'x': df_filtered_ville['Surface_Total'],
                    'y': df_filtered_ville['Prix_en_DH_scaled'],
                    'mode': 'markers',
                    'marker': {
                        'size': 8,
                        'opacity': 0.7,
                        'color': 'blue'  # Couleur des points
                    },
                    'name': 'Données réelles'
                },
                {
                    'x': df_filtered_ville['Surface_Total'],
                    'y': slope * df_filtered_ville['Surface_Total'] + intercept,
                    'mode': 'lines',
                    'line': {
                        'color': 'red',
                        'width': 3,
                        'dash': 'dash'  # Type de ligne en pointillés
                    },
                    'name': 'Ligne de régression linéaire'
                }
            ],
            'layout': {
                'title': f'Relation entre Surface Totale et Prix à {ville} (Prix divisé par 1 000 000 DHS)',
                'xaxis': {'title': 'Surface Totale en m²', 'range': [0, 3000]},
                'yaxis': {'title': 'Prix en DH (divisé par 1 000 000 DHS)'},
                'hovermode': 'closest'  # Mode d'affichage au survol
            }
        }

        # Graphique de la distribution des prix par localisation
        bar_fig_ville = {
            'data': [
                {
                    'x': df_filtered_ville['Localisation'],
                    'y': df_filtered_ville['Prix_en_DH_scaled'],
                    'type': 'bar',
                    'marker': {'color': 'green'},  # Couleur des barres
                }
            ],
            'layout': {
                'title': f'Distribution des Prix à {ville}',
                'xaxis': {'title': 'Localisation'},
                'yaxis': {'title': 'Prix en DH (divisé par 1 000 000 DHS)'},
                'barmode': 'group',  # Mode d'affichage des barres
            }
        }

        # Préparer les données pour le pie chart
        nombre_salles_bain = df_filtered_ville['Nombre de Salles de Bain'].value_counts()
        labels = [f'{nb} salle{"s" if nb > 1 else ""}' for nb in nombre_salles_bain.index]
        sizes = nombre_salles_bain.values

        pie_chart_ville_fig = {
            'data': [go.Pie(labels=labels, values=sizes, hole=0.3)],
            'layout': {
                'title': f'Repartition du Nombre de Salles de Bain à {ville}',
            }
        }

        # Graphiques box plots
        box_plot_chambres = create_box_plot_chambres(df, ville)
        box_plot_surface = create_box_plot_surface(df, ville)
        box_plot_prix = create_box_plot_prix(df_filtered_ville, ville)

        return scatter_fig_ville, bar_fig_ville, pie_chart_ville_fig, box_plot_chambres, box_plot_surface, box_plot_prix

    # # Exécuter l'application Dash
    # if __name__ == '__main__':
    #     app.run_server(host='0.0.0.0', port=8051, debug=True)
    #     # Code pour exécuter l'application Dash
    #     print("Application Dash exécutée")    
    app.run_server(host='0.0.0.0', port=8050, debug=True)
    while True:
        time.sleep(10)
def LoadSql():
    import pymysql
    import pandas as pd
    
    # Charger les données du fichier CSV
    df = preparation()
    df = df.rename(columns={
        'Surface Total': 'Surface_Total',
        'Nombre de Salles de Bain': 'Nombre_de_Salles_de_Bain',
        'Nombre de Chambres': 'Nombre_de_Chambres',
        'Nombre d\'Étage': 'Nombre_d_Etage',
        'Âge du Bien': 'Age_du_Bien',
        'Surface Habitable': 'Surface_Habitable',
        'Nombre de Salons': 'Nombre_de_Salons',
        'Prix': 'Prix_en_DH'
    })

    # Établir une connexion à MySQL
    connection = pymysql.connect(host='mysql',  # Nom du service Docker
                                 user='root',
                                 password='Hamza@123',
                                 database='immobiledata',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Créer la table si elle n'existe pas
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS donnees_immobilieres (
                id INT AUTO_INCREMENT PRIMARY KEY,
                description VARCHAR(255),
                localisation VARCHAR(255),
                prix_en_dh VARCHAR(255),
                surface_total VARCHAR(255),
                nombre_de_salles_de_bain VARCHAR(255),
                nombre_de_chambres VARCHAR(255),
                type VARCHAR(255),
                secteur VARCHAR(255),
                nombre_d_etages VARCHAR(255),
                age_du_bien VARCHAR(255),
                surface_habitable VARCHAR(255),
                nombre_de_salons VARCHAR(255)
            );
            """
            cursor.execute(create_table_sql)

            # Parcourir chaque ligne du DataFrame et insérer dans la table MySQL
            for index, row in df.iterrows():
                # Handle NaN values in all columns
                row = row.fillna('')  # Replace NaN with empty string for all columns
                sql = """INSERT INTO donnees_immobilieres (description, localisation, prix_en_dh, surface_total,
                        nombre_de_salles_de_bain, nombre_de_chambres, type, secteur, nombre_d_etages,
                        age_du_bien, surface_habitable, nombre_de_salons)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, (row['Description'], row['Localisation'], row['Prix_en_DH'], row['Surface_Total'],
                                     row['Nombre_de_Salles_de_Bain'], row['Nombre_de_Chambres'], row['Type'],
                                     row['Secteur'], row['Nombre_d_Etage'], row['Age_du_Bien'], row['Surface_Habitable'],
                                     row['Nombre_de_Salons']))
        
        # Valider la transaction
        connection.commit()
        print("Les données ont été importées dans MySQL avec succès.")

    except Exception as e:
        print(f"Erreur lors de l'importation des données : {e}")

    finally:
        # Fermer la connexion
        connection.close()




# Définir le DAG Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':pendulum.today('UTC').subtract(days=1),
    'retries': 1,
}

dag = DAG(
    'RealEstateDataPipeline',
    default_args=default_args,
    description='A simple DAG for real estate data extraction and processing',
    schedule=None,
)

linkextraction_task = PythonOperator(
    task_id='linkextraction',
    python_callable=linkextraction,
    dag=dag,
)

extraction_task = PythonOperator(
    task_id='ExtractionImmobiliers',
    python_callable=ExtractionImmobiliers,
    dag=dag,
)

preparation_task = PythonOperator(
    task_id='preparation',
    python_callable=preparation,
    dag=dag,
)

dash_app_task = PythonOperator(
    task_id='DashApp',
    python_callable=DashApp,
    dag=dag,
)

load_sql_task = PythonOperator(
    task_id='LoadSql',
    python_callable=LoadSql,
    dag=dag,
)

linkextraction_task >> extraction_task
extraction_task >> preparation_task
preparation_task >> [dash_app_task, load_sql_task]
