

$('.facet-filter').change(function () {
    var model = $('#model').val()
    var experiment = $('#experiment').val()
    var target = "/facet-filter/" + model + '/' + experiment
    $.get(target, function (data) {
        // Handle data from server and populate dropdown menus
        // Declare vars
        var i,j
        var model_list
        var experiment_list
        // Initialise innerHTML strings for dropdown with All option
        model_list = "<option value='All'> All </option>"
        experiment_list = "<option value='All'> All </option>"
        // Loop JSON to build html strings
        for (i=0; i<data.model.length; i++){
            model_list = model_list.concat("<option value=" + data.model[i] + ">" + data.model[i] + "</option>")
        }
        for (j=0; j<data.experiment.length; j++){
            experiment_list = experiment_list.concat("<option value=" + data.experiment[j] + ">" + data.experiment[j] + "</option>")
        }
        // Populate selection dropdowns
        $('#model').html(model_list)
        $('#experiment').html(experiment_list)
        // Choose selected value
        $('#model').val(model)
        $('#experiment').val(experiment)
    })
})
$('#clear_filters').click(function () {
    $('#model').val('All').trigger('change')
    $('#experiment').val('All').trigger('change')
})



