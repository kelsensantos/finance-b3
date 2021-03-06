# noinspection PyUnresolvedReferences
import chromedriver_binary
import tempfile
# módulos externos
from decouple import config
from selenium import webdriver


# Driver para uso do Selenium
class ChromeDriver:

    temporary_dir = tempfile.mkdtemp()

    def __new__(cls, show_chrome=config('SHOW_CHROME', cast=bool)):
        if show_chrome:
            return ChromeDriver.__configure_driver(headless=False)
        return ChromeDriver.__configure_driver()

    @staticmethod
    def __configure_driver(headless=True):
        # configurações do Driver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        if headless:
            chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        # habilita possibilidade de lidar com arquivos e diretórios temporários
        prefs = {"download.default_directory": ChromeDriver.temporary_dir}
        chrome_options.add_experimental_option("prefs", prefs)
        # Cria o driver com Selenium
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(20)
        return driver

