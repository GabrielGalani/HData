# Importações
from pathlib import Path
from jinja2 import Template
from datetime import datetime
import shutil
import os


def move_to_bronze(path_folder, destination):
    template = Template("{{ filename }}_extractDate={{ date }}.xlsx")
    for file in os.listdir(path_folder):
        if "Dados Recebidos" in file:
            path_file = os.path.join(path_folder, file)

            # Substituindo os espaços
            filename = str(file).replace('-', ' ').replace(' ', '_')
            date = datetime.now().strftime("%Y-%m-%d")


            new_file_name = template.render(filename=filename, date=date)

            # Caminho final do arquivo
            destination_path = os.path.join(destination, new_file_name)


            # Criando pasta no datalake
            Path(destination).mkdir(parents=True, exist_ok=True)

            # Copiando arquivo
            shutil.copy(path_file, destination_path)
            return True


if __name__ == "__main__": 
    path_folder = r"C:\Users\gabri\OneDrive\Documentos\Projetos\Case 1 - Dados Fin"
    destination = r'C:\Users\gabri\OneDrive\Documentos\Projetos\Datalake\bronze\dados_recebidos'

    move_to_bronze(path_folder, destination)