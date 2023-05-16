# Databricks notebook source
def dc10(env):

    if env.lower()=='dev' or env.lower()=='qa':

        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.175"
        portnumber = "1810"
        db = "Wmdc10q1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"
        
        return (username,password,connection_string)

    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.160"
        portnumber = "1810"
        db = "WMDC10P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)

    

def dc12(env):
    if env.lower()=='dev' or env.lower()=='qa':
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.186"
        portnumber = "1812"
        db = "Wmdc12q1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username,password,connection_string) 

    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "72.20.89.161"
        portnumber = "1812"
        db = "WMDC12P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)    
   
def dc14(env):

    if env.lower()=='dev' or env.lower()=='qa':
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.170"
        portnumber = "1814"
        db = "wmdc14q1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username,password,connection_string)


    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.159"
        portnumber = "1814"
        db = "WMDC14P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)        

def dc22(env):
    if env.lower()=='dev' or env.lower()=='qa' :
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.175"
        portnumber = "1822"
        db = "Wmdc22q1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username,password,connection_string)


    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.160"
        portnumber = "1822"
        db = "WMDC22P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)       

def dc36(env):
    if env.lower()=='dev' or env.lower()=='qa' :
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.170"
        portnumber = "1836"
        db = "wmdc36q1"
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username,password,connection_string)
     
    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.159"
        portnumber = "1836"
        db = "WMDC36P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)       
 




def dc38(env):
    if env.lower()=='dev' or env.lower()=='qa':
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.186"
        portnumber = "1838"
        db = "Wmdc38q1"
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username,password,connection_string)

    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.161"
        portnumber = "1838"
        db = "WMDC38P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)    

def dc41(env):      
    if env.lower()=='dev' or env.lower()=='qa' :
        username = "SVC_BD_ORA_NP_READ"
        hostname = "172.17.89.175"
        portnumber = "1841"
        db = "Wmdc41q1"
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_NP_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"

        return (username,password,connection_string)      


    if env.lower()=='prod' :
        username = "SVC_BD_ORA_P_READ"
        hostname = "172.20.89.160"
        portnumber = "1841"
        db = "WMDC41P1" 
        password = dbutils.secrets.get(scope = "SVC_BD_ORA_P_READ" , key = f"{db}_password")
        connection_string = f"jdbc:oracle:thin:@//{hostname}:{portnumber}/{db}.world"   

        return (username,password,connection_string)        

def getConfig(DC_NBR,env):
    select={
       'dc10': dc10(env),
       'dc12': dc12(env),
       'dc14': dc14(env),
       'dc22': dc22(env),
       'dc36': dc36(env),
       'dc38': dc38(env),
       'dc41': dc41(env)
       }
    return select.get(DC_NBR)

