#!/usr/bin/python                                                                                                                     
import time
import os
import random
import subprocess
import ConfigParser
import sys
import timeit

siteList = ["UCSD", "Nebraska", "Wisconsin", "FNAL", "Vanderbilt", "Caltech"]
cpus = [1, 1, 1,1 ,1 ,1 ,1 ,1 ,1 ,1,1,1,2,4]
memorys = [1024, 1024, 1024, 1024, 1024, 2048,2048,2048,3046,1024]

def parseConfiguration(configFile):
    config = ConfigParser.ConfigParser()
    config.readfp(open(configFile))
    configAttributes = {}
    configAttributes["Submit"]={}
    configAttributes["Submit"]["Hostname"] = config.get("Submit", "Hostname")
    configAttributes["Submit"]["TargetRunningJobs"] = int(config.get("Submit", "TargetRunningJobs"))
    configAttributes["Submit"]["TargetIdleJobs"] = int(config.get("Submit", "TargetIdleJobs"))
    configAttributes["Submit"]["MaxUser"] = int(config.get("Submit", "MaxUser"))
    configAttributes["Submit"]["AverageJobsToSubmit"] = int(config.get("Submit", "AverageJobsToSubmit"))
    configAttributes["Submit"]["DeviationJobsToSubmit"] = int(config.get("Submit", "DeviationJobsToSubmit"))
    configAttributes["Submit"]["AvgTimeBetweenSub"] = float(config.get("Submit", "AverageTimeBetweenSubmissions"))
    configAttributes["Submit"]["DevTimeBetweenSub"] = float(config.get("Submit", "DevTimeBetweenSubmissions"))
    configAttributes["Job"]={}
    configAttributes["Job"]["AverageTime"] = int(config.get("Job", "AverageTime"))
    configAttributes["Job"]["DeviationTime"] = int(config.get("Job", "DeviationTime"))
    return configAttributes


#Return the idle and running jobs in a schedd
def IdleRunningJobs(scheddname):
    cmd = "condor_status -schedd %s -autoformat 'TotalIdleJobs' 'TotalRunningJobs' 'TotalLocalJobsIdle'" % (scheddname)
    p = subprocess.Popen(cmd , shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    output = p.communicate()
    result = output[0].split(" ")
    IdleJobs = result[0]
    RunningJobs = result[1]
    return (int(IdleJobs), int(RunningJobs))

#Return a randomized list of size Numbersumbissions with average time to sleep between submissions 
def randomTimeBetweenSubmissions(numberSubmissions, averageTime, deviation):
    timeSubmissions = []
    for submitTime in range(0, numberSubmissions):
        timeSubmissions.append(random.normalvariate(averageTime,deviation))
    return timeSubmissions

#returns a random list of size number users and each user is from test0 to testmaxUsers
def randomListUsers(numberUsers, maxUsers):
    userList = []
    for user in range(0, numberUsers):
        userList.append("test"+str(random.randrange(0, maxUsers)))
    return userList
                                                                                                                   
def getRandomNumberSleep(average, deviation):
    return random.normalvariate(average,deviation)

def getRandomJobsToSubmit(average, deviation):
    return int(random.normalvariate(average,deviation))

#Submits a job as user, that will sleep for sleeptime seconds                                                
def submitJob(user, sleeptime):
    adString = getClassAdForJob()
    (cpu, memory, sites) = getClassAdForJob()
    cmd = "/dataScaleTests/submitfiles/submitScript.sh %s %d %s %d %d" % (user, sleeptime, sites, cpu, memory)
    p = subprocess.Popen(cmd , shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = p.communicate()
    result = p.returncode    
    if not (result == 0):
        print output
    return result

def getClassAdForJob():
    cpu = random.choice(cpus)
    memory = random.choice(memorys)
    random.shuffle(siteList)
    sites= ",".join(siteList)
    return (cpu, memory, sites)

# Creates a list of triples with each triple being (user, timebetweensubmission, jobsleeptime)
def createListSubmissionParameters(config, jobsToSubmit):
    parameterList = []
    users = randomListUsers(jobsToSubmit, config["Submit"]["MaxUser"])
    timeBetweenSubs = randomTimeBetweenSubmissions(jobsToSubmit, 
                                                   config["Submit"]["AvgTimeBetweenSub"], 
                                                   config["Submit"]["DevTimeBetweenSub"])
    for job in range(0, jobsToSubmit):
        jobsleepTime = getRandomNumberSleep(config["Job"]["AverageTime"], config["Job"]["DeviationTime"])
        parameter = (users[job], timeBetweenSubs[job], jobsleepTime)
        parameterList.append(parameter)
    return parameterList

def main():
    config = parseConfiguration('submissionTest.cfg')
    hostname = config["Submit"]["Hostname"]
    targetRunningJobs = config["Submit"]["TargetRunningJobs"]
    targetIdleJobs = config["Submit"]["TargetIdleJobs"]
    (idleJobs, runJobs) = IdleRunningJobs(hostname)
    jobToSubmit = getRandomJobsToSubmit(config["Submit"]["AverageJobsToSubmit"], config["Submit"]["DeviationJobsToSubmit"])
    # If the target running Idle jobs is higher only send some jobs to gather condor_submit average time
    if idleJobs >= targetIdleJobs:
        jobToSubmit = 0
        print "Submiting only %d jobs, Idle Jobs: %d higuer than target: %d" % (jobToSubmit, idleJobs, targetIdleJobs)
    #jobToSubmit = 10
    parameterList = createListSubmissionParameters(config, jobToSubmit)
    condor_submit_failures = 0
    totalTime_condorsub = 0
    print "About to submit %d jobs" % (jobToSubmit)
    jobsSubmitted = 0
    for parameter in parameterList:
        start = time.time()
        exitcode = submitJob(parameter[0], parameter[2])
        if not (exitcode == 0):
            condor_submit_failures += 1
            time.sleep(parameter[1])
            jobsSubmitted += 1
            print "Job submission %d failed" % (jobsSubmitted)
            continue
        jobsSubmitted += 1
        if jobsSubmitted % 50 == 0:#only log each 50 jobs submitted
            print "Submitted job :%d of %d" % (jobsSubmitted, jobToSubmit)
        end = time.time()
        totalTime_condorsub += (end - start)
        time.sleep(parameter[1])
    if jobToSubmit > 0:
        condor_submit_success = float(jobToSubmit - condor_submit_failures)/jobToSubmit*100
        average_condor_sub = totalTime_condorsub*1000/jobToSubmit
        #metric = "average_condor_sub_time"
        # Report the average condor submit time
        #reportGanglia(metric, average_condor_sub, 'float', 'msecs')
        # report the sucess in condor submit                                                                                                                
        #metric = "condor_submit_success"
        #reportGanglia(metric, condor_submit_success, 'float', '%')
    # Report the condor_q time
    #reportGanglia_condor_q()

def reportGanglia(metric, average_condor_sub, mtype, units):
    cmd = "/usr/bin/gmetric --name %s -t %s -v %f -u %s" % (metric, mtype, average_condor_sub, units)
    p = subprocess.Popen(cmd , shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    output = p.communicate()
    print "Reporting metric: %s with value %f" % (metric, average_condor_sub)


def reportGanglia_condor_q():
    cmd = "condor_q"
    p = subprocess.Popen(cmd , shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    start = time.time()
    output = p.communicate()
    end = time.time()
    timecondorq = (end - start)
    metric = "time_condor_q"
    reportGanglia(metric, timecondorq, 'float', 'secs')


if __name__ == "__main__":
    main()
