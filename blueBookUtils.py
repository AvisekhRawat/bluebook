from bs4 import BeautifulSoup
import requests
import urllib3
import json
import os
from os import listdir
import xlwt
import xlrd
from xlwt import Workbook 
from xlrd import open_workbook
import pandas as pd
import warnings
import time
from os.path import isfile, join, isdir, exists


class Bluebook:
    def __init__(self):
        self.url = "https://www.macraesbluebook.com"
        self.headingsDict = {}
        self.SDMDict = {}
        self.companyList = {}
        self.continueCompanyList = {}
        warnings.filterwarnings("ignore")
        self.detailedFieldNames = ['Street','City','State', 'Pincode', 'Country', 'PhoneNo', 'Website', 'Products']
        self.detailedFieldNamesComplete = ['CompanyName','Street','City','State', 'Pincode', 'Country', 'PhoneNo', 'Website', 'Products']
        self.waitTime = 10

        self.bbHandler()


    def getSoup(self,url):
        try:
            if(url[:59] != 'https://www.macraesbluebook.com/search/company.cfm?company='):
                return 

            http = urllib3.PoolManager()
            response = http.request('GET',url)
            page = response.data
            soup = BeautifulSoup(page, 'html.parser')
            return soup
        except:
            
            print('Error in Connection ......')
            time.sleep(self.waitTime)
            return self.getSoup(url)

    def bbScrapMain(self):

        soup = self.getSoup(self.url)
        headings = soup.find_all('td',class_="td_tab_index")
        for heading in headings:
            head = heading.get_text().strip()
            if(heading.find('a') ):
                h = heading.find('a', class_= 'a_mbb').get('href')
                if(head != ''):
                    self.headingsDict[head] = self.url + h 
                                
        

    def bbScrapSDM(self, head, url):
        soup = self.getSoup(url) 
        headings = soup.find_all('a',class_="alinks3")
        linkDict = {}
        for  heading in headings:
            name = heading.get_text().strip()
            link = heading.get('href')
            linkDict[name] = self.url + link

        self.SDMDict[head] = linkDict    


    def bbScrapCompanyList(self, name, url):
        soup = self.getSoup(url)
        self.bbScrapCompanyListPage(name, soup)        
        pass
        total = int(soup.find('span',class_ = 'headlineq').get_text().split()[-1])
        numOfPages = (total//25) + 1
        print('{} : {}'.format(name,total))
        for i in range(1,numOfPages):
            nextPageUrl = url+'-'+str(i)
            soup = self.getSoup(nextPageUrl)
            self.bbScrapCompanyListPage(name, soup)


    def bbScrapCompanyListPage(self, name, soup):
        dic = {}
        if(name in self.companyList.keys()):
            dic = self.companyList[name]
        headings = soup.find_all('a',class_="alinksListing")        
        for heading in headings:
            key = heading.get_text().strip()
            value = self.url + heading.get('href')
            dic[key] = value

        self.companyList[name] = dic        
         

    def bbScrapLander(self, url):
        companyDetail = {}
        try:
            soup = self.getSoup(url)
            details = soup.find_all('div',id = 'colum')
            addr = details[0].find('div', itemprop = 'address')        
        except:
            pass
        try:
            companyDetail['Street'] = addr.find('span', itemprop = "streetAddress").get_text().strip()
        except:
            companyDetail['Street'] = ''

        try:
            companyDetail['City'] = addr.find('span', itemprop = "addressLocality").get_text().strip()
        except:
            companyDetail['City'] = ''

        try:
            companyDetail['State'] = addr.find('span', itemprop = "addressRegion").get_text().strip()
        except:
            companyDetail['State'] = ''

        try:
            companyDetail['Pincode'] = addr.find('span', itemprop = "postalCode").get_text().strip()
        except:
            companyDetail['Pincode'] = ''

        try:
            companyDetail['Country'] = addr.find('meta', itemprop = "addressCountry").get('content').strip()
        except:
            companyDetail['Country'] = ''
        try:
            companyDetail['PhoneNo'] = details[1].find('span',itemprop="telephone").get_text().strip()
        except:
            companyDetail['PhoneNo'] = 0 #details[1].find('span',itemprop="telephone").get_text().strip()
        
        try:
            companyDetail['Website'] = details[1].find('a', class_ = "offsite").get('href')
        except:
            companyDetail['Website'] = ''

        try:
            companyDetail['Products'] = soup.find('p', itemprop="description").get_text().strip().replace('\n', '.').replace('\r', '.').strip()                        

        except:
            companyDetail['Products'] = ''
        
        return companyDetail

    def analyze_hdf(self):
        hdfPath = 'HDFFiles/'
        filesInFolder = [f for f in listdir(hdfPath) if isfile(join(hdfPath, f))]
        print(filesInFolder)
        
        for fil in filesInFolder:
            hdfFile = pd.HDFStore(hdfPath + fil)
            
            for sdm in hdfFile.keys():

                print(hdfFile[sdm])        
    
    def getTime(self):
        t = time.localtime()
        return '{}:{}:{}'.format(t.tm_hour, t.tm_min, t.tm_sec)
    


    def step1(self):
        print('populating headingsDict.')
        self.bbScrapMain()
        print('self.headingsDict Dumping begin....')
        with open('headingsDict.json', 'w') as fileW:
            json.dump(self.headingsDict , fileW, indent = 4)            
        print('Dumping end....')
        
    def step2(self):
        print('Reading headingDict from dumps')
        with open('headingsDict.json', 'r') as fileW:
            self.headingsDict = json.load(fileW)
        print('headingDict loaded...Records : ', len(self.headingsDict))            
        print('populating SDMDict.')

        count = 0
        lenbbscrapSDM = len(self.headingsDict)
        for head in self.headingsDict:
            self.bbScrapSDM(head, self.headingsDict[head])
            print('({}/{})'.format(count,lenbbscrapSDM))
            count += 1
        print('self.SDMDict Dumping begin....')
        with open('SDMDict.json', 'w') as fileW:
            json.dump(self.SDMDict , fileW, indent = 4)            
        print('Dumping end....')
        
    def step3(self):
        print('Reading SDMDict from dumps')
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            
        print('populating companyDict.')
        
        totalCompanies = 0
        sdmCount = 0
        for sdm in self.SDMDict:
            print('........................')
            print("SDM : ",sdm, sdmCount)
            c = 0
            sdmCount += 1
            self.continueCompanyList[sdm] = [] 
            for i in self.SDMDict[sdm]:
                c += 1
                totalCompanies += 1
                print('{} : {}\t ({} , {})'.format(i, self.SDMDict[sdm][i], c,totalCompanies))
                self.bbScrapCompanyList(i, self.SDMDict[sdm][i])
                with open('tempCompanyList.json', 'w') as fileW:
                    json.dump(self.companyList , fileW, indent = 4)            
                self.continueCompanyList[sdm].append(i)
                with open('continueCompanyList.json', 'w') as fileW:
                    json.dump(self.continueCompanyList , fileW, indent = 4)            
                
                #print('CCL',self.continueCompanyList)
            print('************Dumping : {} *****************'.format(sdm))
            fileName = 'CompanyLists/' + sdm + '.json'
            with open(fileName, 'w') as fileW:
                json.dump(self.companyList , fileW, indent = 4)    
            self.companyList = {}

    def step3_continue(self):
        with open('continueCompanyList.json', 'r') as fileW:
            self.continueCompanyList = json.load(fileW)
        print('continueCompanyList loaded...Records : ', len(self.continueCompanyList))            

        with open('tempCompanyList.json', 'r') as filex:
            self.companyList = json.load(filex)
        print('tempCompanyList loaded...Records : ', len(self.companyList))            

        lastHead = list(self.continueCompanyList.keys())[-1]
        print('lastHead ',lastHead)
        lastSdm = self.continueCompanyList[lastHead][-1]       
        print('LastSDM ',lastSdm)
        
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            
        
        totalCompanies = 0
        sdmCount = 0

        foundFlag = False
        foundFlag2 =False
        for sdm in self.SDMDict:
            print('........................')
            print("SDM : ",sdm, sdmCount)
            c = 0
            sdmCount += 1
            if(sdm == lastHead):
                foundFlag2 =True
            if(foundFlag2):
                if(sdm not in self.continueCompanyList):
                    self.continueCompanyList[sdm] = []
                for i in self.SDMDict[sdm]:
                    if(i == lastSdm):
                        foundFlag = True
                        print('Flag found',i)
                    if(foundFlag):
                        if(i in self.continueCompanyList[sdm] ):
                            self.continueCompanyList[sdm].remove(i)    
                        c += 1
                        totalCompanies += 1
                        print('{} : {}\t ({} , {}) [{}]'.format(i, self.SDMDict[sdm][i], c,totalCompanies, len(self.SDMDict[sdm])))
                        self.bbScrapCompanyList(i, self.SDMDict[sdm][i])
                        with open('tempCompanyList.json', 'w') as fileW:
                            json.dump(self.companyList , fileW, indent = 4)            
                        self.continueCompanyList[sdm].append(i)
                        with open('continueCompanyList.json', 'w') as fileW:
                            json.dump(self.continueCompanyList , fileW, indent = 4)
                print('************Dumping : {} *****************'.format(sdm))
                fileName = 'CompanyLists/' + sdm + '.json'
                with open(fileName, 'w') as fileW:
                    json.dump(self.companyList , fileW, indent = 4)    
                self.companyList = {}



        print('self.companyList Dumping begin....')
        with open('companyList.json', 'w') as fileW:
            json.dump(self.companyList , fileW, indent = 4)            
        print('Dumping end....')


    def step4(self):
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            

        totalCompanies = 0
        for sdm in self.SDMDict:
            fileName = 'CompanyLists2/' + sdm + '.json'
            sdmFile = ''
            try:
                with open(fileName) as filee:
                    sdmFile = json.load(filee)
                total = 0
                tempCList = {}
                print("{}: {} subHeads".format(sdm, len(sdmFile)) ) 
                subHeadCount = 1
                jsonFile = {}
                for comps in sdmFile:
                    print('\t\tSubHead : {}, Companies: {}   ({}/{})'.format(comps, len(sdmFile[comps]), subHeadCount,len(sdmFile)))
                    jsonFile[comps] = {}
                    subHeadCount += 1
                    tempCList[comps] = []
                    c = 0
                    for comp in sdmFile[comps]:
                        c += 1
                        total += 1
                        dic = self.bbScrapLander(sdmFile[comps][comp])
                        dic['products'] = dic['products'].replace('\n', '.').replace('\r', '.')                        
                        jsonFile[comps][comp] = dic
                        print('{}  ({} / {} )  {}'.format(comp, c, len(sdmFile[comps]) ,total))
                        
                        tempCList[comps].append(comp)                    
                        
                        tempCListName = 'temp/'+sdm+'_cL.json'
                        with open(tempCListName, 'w') as tcln:
                            json.dump(tempCList, tcln, indent=4)
                        
                        tempCListJSONName = 'temp/'+sdm+'_cLJ.json'
                        with open(tempCListJSONName, 'w') as tcljn:
                            json.dump(jsonFile, tcljn, indent=4)
            
                    jsonFileName = 'CompanyListsJSON/'+sdm+'_c.json'
                    with open(jsonFileName, 'w') as filese:
                        json.dump(jsonFile,filese, indent = 4 )
                tempCListName = 'temp/'+sdm+'_cL.json'
                tempCListJSONName = 'temp/'+sdm+'_cLJ.json'
                
                if os.path.exists(tempCListName):
                    os.remove(tempCListName)
                else:
                    print("The file does not exist",tempCListName)

                if os.path.exists(tempCListJSONName):
                    os.remove(tempCListJSONName)
                else:
                    print("The file does not exist",tempCListJSONName)                
            except:
                pass
                #print('ERROR in reading {}'.format(fileName))


    def step4_continue(self):
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            
        mypath = 'CompanyListsJSON/'
        filesInFolder = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        print(filesInFolder)
        return 


        totalCompanies = 0
        for sdm in self.SDMDict:
            sdmFile = sdm+'_c.json'
            if(sdmFile in filesInFolder ):
                continue
            fileName = 'CompanyLists2/' + sdm + '.json'            
            sdmFile = ''
            continueCompanyList  = {}
            continueCompanyJSONList  = {}
            try:
                with open('temp/'+sdm+'_cL.json') as filee:
                    continueCompanyList = json.load(filee)
            except:
                pass

            try:
                with open('temp/'+sdm+'_cLJ.json') as filee:
                    continueCompanyJSONList = json.load(filee)
            except:
                pass            

            try:
                with open(fileName) as filee:
                    sdmFile = json.load(filee)
                total = 0
                tempCList = {}
                print("{}: {} subHeads".format(sdm, len(sdmFile)) ) 
                subHeadCount = 1
                jsonFile = {}

                lastSDM = list(sdmFile.keys())[0]
                lastComp = list(sdmFile[list(sdmFile.keys())[0]].keys())[0]
                if(len(continueCompanyList) > 0):
                    lastSDM = list(continueCompanyList.keys())[-1]
                    lastComp = list(sdmFile[list(continueCompanyList.keys())[-1]].keys())[-1]
                print(lastSDM,lastComp)
                return

                flag1 = False
                flag2 = False
                for comps in sdmFile:

                    print('\t\tSubHead : {}, Companies: {}   ({}/{})'.format(comps, len(sdmFile[comps]), subHeadCount,len(sdmFile)))
                    jsonFile[comps] = {}
                    subHeadCount += 1
                    tempCList[comps] = []
                    c = 0
                    for comp in sdmFile[comps]:
                        c += 1
                        total += 1
                        dic = self.bbScrapLander(sdmFile[comps][comp])
                        dic['products'] = dic['products'].replace('\n', '.').replace('\r', '.').strip()                        
                        jsonFile[comps][comp] = dic
                        print('{}  ({} / {} )  {}'.format(comp, c, len(sdmFile[comps]) ,total))
                        
                        tempCList[comps].append(comp)                    
                        
                        tempCListName = 'temp/'+sdm+'_cL.json'
                        with open(tempCListName, 'w') as tcln:
                            json.dump(tempCList, tcln, indent=4)
                        
                        tempCListJSONName = 'temp/'+sdm+'_cLJ.json'
                        with open(tempCListJSONName, 'w') as tcljn:
                            json.dump(jsonFile, tcljn, indent=4)
            
                    jsonFileName = 'CompanyListsJSON/'+sdm+'_c.json'
                    with open(jsonFileName, 'w') as filese:
                        json.dump(jsonFile,filese, indent = 4 )
                
                tempCListName = 'temp/'+sdm+'_cL.json'
                tempCListJSONName = 'temp/'+sdm+'_cLJ.json'
                
                if os.path.exists(tempCListName):
                    os.remove(tempCListName)
                else:
                    print("The file does not exist",tempCListName)

                if os.path.exists(tempCListJSONName):
                    os.remove(tempCListJSONName)
                else:
                    print("The file does not exist",tempCListJSONName)


            except:
                pass
                #print('ERROR in reading {}'.format(fileName))

    def analyzer(self):
        with open('CompanyLists2/MACHINERY & EQUIPMENT, INDUSTRIAL, WHOLESALE.json') as f:
            sdmFile = json.load(f)     
        for sdm in sdmFile:
            print(sdm, len(sdmFile[sdm]))
        #print(len(sdmFile))


    def remDD(self):
        
        myPath =  os.getcwd() 
        mypath = join(join(myPath,'HDFFiles'),'ss')

        filesInFolder = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        print(filesInFolder)
        
        for filee in filesInFolder:
            os.remove(join(mypath,filee))

        #os.rmdir('ss')


    def step4_excel(self):
        mypath = 'CompanyLists2/'
        filesInFolder = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        print(filesInFolder)
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            

        for head in self.SDMDict.keys():
            if(head+'.json' in filesInFolder):
                print('Yes', head)
                tempExcel = 'temp/tempExcel.xls'

                wb  = Workbook()
                sdmFile = {}
                fileName = 'CompanyLists2/' + head + '.json'            
                with open(fileName) as filee:
                    sdmFile = json.load(filee)
                lastComp = ''
                lastSdm = ''

                for sdm in sdmFile:
                    print('SDM:', sdm)
                    sheet = wb.add_sheet(sdm) 
                    sheet.write(0, 0, 'CompanyName')                    
                    for i in range(len( self.detailedFieldNames )):
                        sheet.write(0, i+1, self.detailedFieldNames[i])
                        
                    row = 0
                    for comp in sdmFile[sdm]:
                        print(comp, sdmFile[sdm][comp])                    
                        dic = self.bbScrapLander(sdmFile[sdm][comp])
                        if(len(list(dic.keys())) > 0):
                            row += 1
                            sheet.write(row, 0, comp)
                            col = 1
                            for f in dic:
                                sheet.write(row, col, dic[f])
                                col +=1
                            
                    wb.save(tempExcel) 
                     

                excelFileName = 'Excels/' +head+ '.xls'
                wb.save(excelFileName) 
                wb= Workbook()
                wb.add_sheet('Sheet1')
                wb.save(tempExcel)


    def step4_hdf(self):
        mypath = 'CompanyLists2/'
        filesInFolder = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        print(filesInFolder)
        
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            

        
        for head in self.SDMDict.keys():
            if(head+'.json' in filesInFolder):
                print('Yes', head)
                
                hdfFile = pd.HDFStore('HDFFiles/'+head+'.h5')
                sdmFile = {}
                fileName = mypath + head + '.json'            
                with open(fileName) as filee:
                    sdmFile = json.load(filee)
                lastComp = ''
                lastSdm = ''
                sdmCount  = 1
                print('Total sdm in File  = {}'.format(len(list(sdmFile.keys()))))
                companyCountTotal = 1
                for sdm in sdmFile:
                    print('SDM: {} {} Companies: {}'.format(sdm,sdmCount, len(sdmFile[sdm])) )
                    df = pd.DataFrame(columns = self.detailedFieldNamesComplete)
                    hdfFile[sdm] = df
                    row = 0
                    compCount = 1
                    for comp in sdmFile[sdm]:
                        print('{} - {} \t\tSdm [{}/{}]\t CompInSdm({}/{}) \t Total: {}'.format(self.getTime(), comp, sdmCount, len(list(sdmFile.keys())), compCount, len(sdmFile[sdm]), companyCountTotal))
                        companyCountTotal += 1

                        dic = self.bbScrapLander(sdmFile[sdm][comp])
                        if(len(list(dic.keys())) > 0):
                            fullList =  [comp]+ list(dic.values())
                            df.loc[row]  = fullList
                            row += 1
                            compCount +=1
                            hdfFile[sdm] =df
                    sdmCount += 1                    

    def step4_hdf2(self):
        mypath = 'CompanyLists2/'
        filesInFolder = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        print(filesInFolder)
        
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            

        
        for head in self.SDMDict.keys():
            if(head+'.json' in filesInFolder):
                print('Yes', head)
                
                myPath =  os.getcwd() 
                headDir = join(join(myPath,'HDFFiles'),head)
                try:
                    os.mkdir(headDir)
                except:
                    pass    

                sdmFile = {}
                fileName = mypath + head + '.json'            
                with open(fileName) as filee:
                    sdmFile = json.load(filee)
                lastComp = ''
                
                filesInFolder2 = [f for f in listdir(headDir) if (isfile(join(headDir, f)) and f.split('.')[-1]=='h5')]
                sdmFileNames =  [f for f in sdmFile]
                lastSdm = sdmFileNames[0]
                #print('Names in sdm: ',sdmFileNames)

                if(len(filesInFolder2) != 0):
                    f= ''
                    for fi in sdmFileNames:
                        try:
                            if(exists(join(headDir, fi+'.h5'))):
                                with pd.HDFStore(join(headDir, fi+'.h5'))  as hdF:
                                    pass
                                lastSdm = fi
                        except:
                            f = fi
                            break
                    if(f == ''):
                        print('All Fine')
                        if(len(sdmFileNames) == len(filesInFolder2)):
                            #return
                            continue
                    else:
                        print('Problem in ', f)
                        os.remove(join( headDir, f+'.h5'))
                        lastSdm = f
                    print('Last SDM ',lastSdm)                 
                    
                
                sdmCount  = 1
                print('Total sdm in File  = {}'.format(len(list(sdmFile.keys()))))
                companyCountTotal = 1
                flag1 = False
                #print(sdmFile.keys())
                for sdm in sdmFile:
                    if(sdm == lastSdm.split('.')[0]):
                        flag1 = True
                    if(flag1 == True):
                        print('SDM: {} {} Companies: {}'.format(sdm,sdmCount, len(sdmFile[sdm])) )
                        df = pd.DataFrame(columns = self.detailedFieldNamesComplete)
                        hdfFile = pd.HDFStore( join(headDir, (sdm +'.h5')))
                        
                        hdfFile[sdm] = df
                        row = 0
                        compCount = 1
                        for comp in sdmFile[sdm]:
                            print('{} - {} \t\tSdm [{}/{}]\t CompInSdm({}/{}) \t Total: {}'.format(self.getTime(), comp, sdmCount, len(list(sdmFile.keys())), compCount, len(sdmFile[sdm]), companyCountTotal))
                            companyCountTotal += 1

                            dic = self.bbScrapLander(sdmFile[sdm][comp])
                            if(len(list(dic.keys())) > 0):
                                fullList =  [comp]+ list(dic.values())
                                df.loc[row]  = fullList
                                row += 1
                                compCount +=1
                                hdfFile[sdm] =df
                        sdmCount += 1
                    
                        hdfFile.close()                        
         
    def step4_hdf3(self):
        mypath = 'CompanyLists2/'
        filesInFolder = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        print(filesInFolder)
        
        with open('SDMDict.json', 'r') as fileW:
            self.SDMDict = json.load(fileW)
        print('SDMDict loaded...Records : ', len(self.SDMDict))            

        
        for head in self.SDMDict.keys():
            if(head+'.json' in filesInFolder):
                print('Yes', head)
                
                myPath =  os.getcwd() 
                headDir = join(join(myPath,'HDFFiles'),head)
                try:
                    os.mkdir(headDir)
                except:
                    pass    

                sdmFile = {}
                fileName = mypath + head + '.json'            
                with open(fileName) as filee:
                    sdmFile = json.load(filee)
                lastComp = ''
                
                filesInFolder2 = [f for f in listdir(headDir) if (isfile(join(headDir, f)) and f.split('.')[-1]=='h5')]
                sdmFileNames =  [f for f in sdmFile]
                lastSdm = sdmFileNames[0]
                #print('Names in sdm: ',sdmFileNames)

                if(len(filesInFolder2) != 0):
                    f= ''
                    for fi in sdmFileNames:
                        try:
                            if(exists(join(headDir, fi+'.h5'))):
                                with pd.HDFStore(join(headDir, fi+'.h5'))  as hdF:
                                    pass
                                lastSdm = fi
                        except:
                            f = fi
                            break
                    if(f == ''):
                        print('All Fine')
                        if(len(sdmFileNames) == len(filesInFolder2)):
                            #return
                            continue
                    else:
                        print('Problem in ', f)
                        os.remove(join( headDir, f+'.h5'))
                        lastSdm = f
                    print('Last SDM ',lastSdm)                 
                    
                
                sdmCount  = 1
                print('Total sdm in File  = {}'.format(len(list(sdmFile.keys()))))
                companyCountTotal = 1
                flag1 = False
                #print(sdmFile.keys())
                for sdm in sdmFile:
                    if(sdm == lastSdm.split('.')[0]):
                        flag1 = True
                    if(flag1 == True):
                        print('SDM: {} {} Companies: {}'.format(sdm,sdmCount, len(sdmFile[sdm])) )
                        df = pd.DataFrame(columns = self.detailedFieldNamesComplete)
                        hdfFile = pd.HDFStore( join(headDir, (sdm +'.h5')))
                        
                        hdfFile['sdm'] = df
                        row = 0
                        compCount = 1
                        for comp in sdmFile[sdm]:
                            print('{} - {} \t\tSdm [{}/{}]\t CompInSdm({}/{}) \t Total: {}'.format(self.getTime(), comp, sdmCount, len(list(sdmFile.keys())), compCount, len(sdmFile[sdm]), companyCountTotal))
                            companyCountTotal += 1

                            dic = self.bbScrapLander(sdmFile[sdm][comp])
                            if(len(list(dic.keys())) > 0):
                                fullList =  [comp]+ list(dic.values())
                                df.loc[row]  = fullList
                                row += 1
                                compCount +=1
                                
                                if(compCount % 100 == 0):
                                    hdfFile['sdm'] =df
                        hdfFile['sdm'] =df
                        sdmCount += 1
                    
                        hdfFile.close()                        
         


    def hdfToExcel(self):
        hdfPath = 'HDFFiles/'
        xlPath = 'Excels/'
        filesInFolder = [f for f in listdir(hdfPath) if isfile(join(hdfPath, f))]
        print(filesInFolder)
        
        for fil in filesInFolder:
            hdfFile = pd.HDFStore(hdfPath + fil)
            wb = Workbook()
            
            for sdm in hdfFile.keys():
                sheetName = sdm[1:32].strip().replace(' ', '').replace('/','')
                sheet = wb.add_sheet(sheetName)
                #print(hdfFile[sdm])        
                df = hdfFile[sdm]
                print('SheetName : ',sheetName, df.shape[0])
                for c in range(len(self.detailedFieldNamesComplete)):
                    sheet.write(0, c, self.detailedFieldNamesComplete[c])  
                        
                row1 = 1
                for row in range(df.shape[0]):
                    for col in range(df.shape[1]):
                        sheet.write(row1, col, df.loc[row][col])  
                    row1 += 1                  
            wb.save(xlPath+fil.split('.')[0]+'.xls')        
            print('Saved ',fil)


    def hdfToExcel2(self):
        hdfPath = 'HDFFiles/'
        xlPath = 'Excels/'
        filesInFolder = [f for f in listdir(hdfPath) if isdir(join(hdfPath, f))]
        print('Generating Excel')
        print(filesInFolder)
        myPath =  os.getcwd() 
        for dirr in filesInFolder:
            
            mypath = join(join(myPath,'HDFFiles'),dirr)
            filesInSDM = [f for f in listdir(mypath) if(isfile(join(mypath, f)) and f.split('.')[-1]=='h5') ]
            print(filesInSDM)        
            #continue
            wb = Workbook()
            
            for filee in filesInSDM:
                sheetName = filee.split('.')[0][:32].strip().replace(' ', '').replace('/','')
                sheet = wb.add_sheet(sheetName)
                
                hdfFile = pd.HDFStore(join(mypath, filee))
                #print('hdfFile Keys ',hdfFile.keys())
                #df = hdfFile[filee.split('.')[0]]
                df = hdfFile['sdm']
                print('SheetName : ',sheetName, df.shape[0])
                for c in range(len(self.detailedFieldNamesComplete)):
                    sheet.write(0, c, self.detailedFieldNamesComplete[c])  
                        
                row1 = 1
                for row in range(df.shape[0]):
                    for col in range(df.shape[1]):
                        sheet.write(row1, col, df.loc[row][col])  
                    row1 += 1 
                hdfFile.close()                     
            wb.save(xlPath+dirr.split('.')[0]+'.xls')        
            print('Saved ',dirr)

    def openHDF(self):
        pass

    def closeHDF(self):
        pass 
    
    def bbHandler(self):
        #self.step4_hdf3()
        #self.hdfToExcel2()
        self.analyzer()
        #self.remDD()
        print('DONE.')