import os
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# selenium version 3.141.0
# urllib3 version 1.26.20

class CscMailUtil:
    def __init__(self, username: str, password: str) -> None:
        options = webdriver.ChromeOptions()
        options.binary_location = "D:/Dev/Chrome/chrome.exe"
        prefs = {"profile.default_content_settings.popups": 0}
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--enable-chrome-browser-cloud-management")
        self.driver = webdriver.Chrome(executable_path="D:/Dev/tools/chromedriver.exe", options=options)
        self.driver.get("https://cas.csc108.com/login")
        # 输入用户名密码并登录

        # self.driver.find_element_by_id("s360_coa").click()
        self.driver.find_element_by_id("qrcode_btn").click()
        self.driver.find_element_by_name("username").clear()
        self.driver.find_element_by_name("username").send_keys(username)
        self.driver.find_element_by_name("password").clear()
        self.driver.find_element_by_name("password").send_keys(password)
        self.driver.find_element_by_id("submit1").click()
        self.driver.get("https://newmail.csc.com.cn/")

    def send_email(self, subject: str, addressee: list, cc_list: list, content: str, files: list = []):
        self.driver.get("https://newmail.csc.com.cn/")
        new_btn = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "j-mlsb")))
        time.sleep(2)
        new_btn.click()

        form_tt = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "j-form-tt")))
        time.sleep(2)
        input_subject = WebDriverWait(form_tt, 10).until(EC.presence_of_element_located((By.NAME, "subject")))
        input_subject.send_keys(subject)
        time.sleep(2)
        input_to = WebDriverWait(form_tt, 10).until(EC.presence_of_element_located((By.NAME, "to")))
        tmp = input_to.get_attribute("class")
        self.driver.execute_script("arguments[0].setAttribute('class', '');", input_to)
        input_to.send_keys(";".join(addressee))
        self.driver.execute_script(f"arguments[0].setAttribute('class', '{tmp}');;", input_to)
        time.sleep(2)

        input_cc = WebDriverWait(form_tt, 10).until(EC.presence_of_element_located((By.NAME, "cc")))
        tmp = input_cc.get_attribute("class")
        self.driver.execute_script("arguments[0].setAttribute('class', '');", input_cc)
        # logging.info(cc_list)
        for cc_item in cc_list:
            input_cc.send_keys(f"{cc_item};")
            # must sleep for else be banned
            time.sleep(1)

        self.driver.execute_script(f"arguments[0].setAttribute('class', '{tmp}');;", input_cc)
        time.sleep(2)

        # edit_area = self.driver.find_element_by_class_name("j-form-edr")
        # edit_area.click()
        # time.sleep(1)

        j_html_menu = self.driver.find_element_by_xpath('//ul[@class="u-list u-list-horizontal j-html-menu"]/li[5]/a')
        j_html_menu.click()
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "j-edit-source")))
        source_editer = self.driver.find_element_by_class_name("j-edit-source")
        source_editer.click()

        self.driver.switch_to.frame(self.driver.find_element_by_class_name("ke-edit-iframe"))
        input_body = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "ke-content")))

        input_body.send_keys(content)
        self.driver.switch_to.default_content()

        soure_close_link = self.driver.find_element_by_class_name("j-close-source-mode")
        soure_close_link.click()
        time.sleep(3)

        if files:
            input_file = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "attachments")))
            for f in files:
                input_file.send_keys(os.path.abspath(f))
                input_file = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "attachments")))

        send_btn = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "j-tbl-send")))
        time.sleep(2)
        send_btn.click()
        # close browser
        time.sleep(3)
        self.driver.quit()

    def upload(self, files: list):
        # click file icon
        filer = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "iconfiler")))
        filer.click()
        # switch frame
        iframe = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
        self.driver.switch_to.frame(iframe)
        # click upload
        upbtn = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#headControlArea > div.fLeft > div.sigbtn_ns.sigbtn_ns_l")))
        upbtn.click()
        # send file names
        input_btn = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#divUploadAttachBigFile")))
        input_btn.send_keys("\n".join(files))

        time.sleep(10)
        # # click confirm
        # confirmer = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.sigbtn_ns.sigbtn_ns_on")))
        # confirmer.click()
        # quit browser
        self.driver.quit()


if __name__ == "__main__":
    target_dt_int = 20250123
    year = target_dt_int // 10000
    sender = CscMailUtil("CSCXXXXXX", "XXXXXX")
    sender.upload(
        files=[
            f"transfer/etf-bar15m/{year}/bar15m-{target_dt_int}.ipc",
            f"transfer/etf-bar1m/{year}/{target_dt_int}.ipc",
        ],
    )
