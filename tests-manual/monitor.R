library(simworkr)
connect()
clearJobs(queue='test')
deleteExperiment('simworkr')

model = function(...) {
    Sys.sleep(2)
    return(TRUE)
}

cases = expand.grid(data='iris', method='model', run=1:100)
submitJobs('simworkr', cases, queue='test', inOrder=TRUE)

jobq('test')

workerFile = file.path(getwd(), 'tests', 'worker.R')
system(sprintf('R --slave -f %s', workerFile), wait=FALSE)

jobs('test')
