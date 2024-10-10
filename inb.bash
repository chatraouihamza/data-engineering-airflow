# apt-get update
# apt-get install -y wget gnupg ca-certificates
# wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
# echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list
# apt-get update
# apt-get install -y google-chrome-stable
# # google-chrome-stable --version

# # # # Télécharger ChromeDriver
# CHROMEDRIVER_URL="https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.182/linux64/chromedriver-linux64.zip"
# CHROMEDRIVER_ZIP="/tmp/chromedriver-linux64.zip"
# INSTALL_DIR="/usr/local/bin"
# wget $CHROMEDRIVER_URL -O $CHROMEDRIVER_ZIP
# # # # Décompresser le fichier
# unzip $CHROMEDRIVER_ZIP -d $INSTALL_DIR
# chmod +x /usr/local/bin/chromedriver
# mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver
# # # Vérifier l'installation
# chromedriver --version
# google-chrome-stable --version

# Nettoyer les fichiers temporaires de apt
rm -rf /var/lib/apt/lists/*

# Mettre à jour les dépôts et installer les dépendances nécessaires
apt-get update
apt-get install -y wget gnupg ca-certificates

# Ajouter la clé de Google Chrome à un fichier de clé GPG
wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/trusted.gpg.d/google.gpg

# Ajouter le dépôt de Google Chrome
echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list

# Mettre à jour les dépôts après avoir ajouté le dépôt de Google Chrome
apt-get update

# Installer Google Chrome stable
apt-get install -y google-chrome-stable

# Vérifier la version de Google Chrome installée
google-chrome-stable --version
