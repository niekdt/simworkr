context('Jobs')
skip_if_not(
    condition = isTRUE(as.logical(Sys.getenv('WITH_REDIS', unset = 'true'))),
    message = 'Redis disabled'
)

test_that('job submission', {
    connect()
    clearJobs(queue='test')
    expect_equal(jobCount(queue='test'), 0)

    model1 = lm
    model2 = lm
    model3 = lm
    cases = expand.grid(data=c('iris', 'iris3'), method=c('model1', 'model2', 'model3'), extra=1:3)
    submitJobs('simworkr', cases, queue='test')
    expect_equal(jobCount(queue='test'), nrow(cases))

    # resubmit jobs
    submitJobs('simworkr', cases, queue='test')
    expect_equal(jobCount(queue='test'), nrow(cases))

    # submit some new jobs
    cases2 = expand.grid(data=c('iris', 'iris3'), method=c('model1', 'model2', 'model3'), extra=1:4)
    submitJobs('simworkr', cases2, queue='test')
    expect_equal(jobCount(queue='test'), nrow(cases2))

    expect_equal(jobQueue(queue='test'), data.table::data.table(Experiment='simworkr', Jobs=24))

    expect_true('test' %in% getJobQueues())

    # faulty case: missing data
    cases = expand.grid(data='simworkrMissingDataset', method=c('model0', 'model1'))
    expect_error(submitJobs('simworkr', cases, queue='test'))

    # faulty case: missing model
    cases = expand.grid(data='iris', method=c('model0', 'model1', 'missingModel'))
    expect_error(submitJobs('simworkr', cases, queue='test'))


    # clean up
    clearJobs(queue='test')
    expect_equal(jobCount(queue='test'), 0)
    disconnect()
})
