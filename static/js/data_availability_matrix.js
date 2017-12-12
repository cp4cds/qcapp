/**
 * Created by vdn73631 on 23/11/2017.
 */
$(window).resize(setVaribleTableHeader())
Mustache.tags = ['[[',']]']


function getCookie(name) {
    // Gets cookies from the browser by name
    var cookieValue = null;
    if (document.cookie && document.cookie !== '') {
        var cookies = document.cookie.split(';');
        for (var i = 0; i < cookies.length; i++) {
            var cookie = jQuery.trim(cookies[i]);
            // Does this cookie string begin with the name we want?
            if (cookie.substring(0, name.length + 1) === (name + '=')) {
                cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                break;
            }
        }
    }
    return cookieValue;
}

function buildSelect(array, name) {
    // Used when building the varibles table. Make sure that fields which only have one option are set as an uneditable
    // input rather than a selection to make it clearer to the user what they can change and what they can't.
    var html, j;

    // If response only has one item render a readonly input
    if (array[name].length === 1){
        return "<input readonly id="+ name + " name="+ name +" value="+ array[name] +">"
    }

    // If array contains more than one element, render a select box. Style to match width of input element
    html = "<select class='variable-table-select' style='width: 152px' id="+ name +" name=" + name + ">";

    // Build options list
    for (j = 0; j < array[name].length; j++) {
        html = html.concat("<option value='"+ array[name][j] +"'>" + array[name][j] + "</option>")
    }

    // Append select to close the element
    html = html.concat("</select>")

    return html
}

function setVaribleTableHeader(){
    // Make sure that the columns in the header for the variables table matches the content.
    var widths = [];
    // Get widths of <td> elements in first row of content
    $('.variable-details tr td').slice(0,3).each(function () {
        widths.push(parseInt($(this).css('width')) + parseInt($(this).css('padding-left')) + parseInt($(this).css('padding-right')))
    });

    // Set the width of the header columns to match content
    $('.static-header th').each(function(index){
        $(this).width(widths[index])
    })
}

function ensembleTags(array){
    // create ensemble tags
    var j;
    var tagstring = "";
    for (j=0; j<array.length; j++){
        tagstring = tagstring.concat("<p class='ensemble-tag'>"+ array[j] + "</p>")
    }
    return tagstring
}

function filterChangeMessage(){
    // When something in the filters changes, reduce the opacity of the results and display a message. Only relevant
    // when the results div is actually visible.
    if ($('#results:visible').length > 0){
        $('#messages').html("<h4 class='text-danger'>Your filters have changed. Click 'Get results' to update your results.</h4>").fadeTo('fast',1)
        $("#results").fadeTo('fast', 0.5)
    }
}

function keepChosenOpen (chosen) {
    if (chosen.data("chosen") != undefined){

        // Keep Chosen box open when selecting.
        var _fn = chosen.data("chosen").result_select;
        chosen.data("chosen").result_select = function(evt) {
              evt["metaKey"] = true;
              evt["ctrlKey"] = true;
              chosen.data('chosen').result_highlight.addClass("result-selected");
              return _fn.call(chosen.data('chosen'), evt);
        };
    }
}

function split_tr(table_body) {

    var body_array = table_body.split("</tr>")

    for (var i = 0; i < body_array.length; i++) {
        body_array[i] = body_array[i] + "</tr>"
    }

    return body_array
}

function indexToDelete(){
    var table_body = split_tr($('.variable-details').html())

    for (var i=0; i<table_body.length; i++){
        if (table_body[i].indexOf(str)){
            return i
        }

    }
    return undefined
}

function removeVariable(rownumber){
    $('#'+ rownumber + '-row').remove()
}

function addVariableRow(row_data){
    var table = $('.variable-details');
    var table_body = split_tr(table.html());

    var view = {
        table_select: buildSelect(row_data, "tables"),
        freq_select: buildSelect(row_data, "frequencies"),
        variable: row_data.variable,
        elements: table_body.length
    };

    var template = $('#variable-row-template').html();
    Mustache.parse(template);

    var row = Mustache.render(template,view);

    table_body.push(row);

    table.html(table_body.join(''))




}

// -------------------------------------------------- Main Code --------------------------------------------------------


// Render variable select box as a chosen dropdown
$('#variable-select').chosen({width:"70%",placeholder_text: "Start typing a variable name or click to see the list"})

// Select all for variables
$(".select-variable").click(function () {
    if ($('.variable-details tr').length < 1) {

        $('#variable-select option').each(function () {
                $('#variable-select').val($(this).val())
                $('#addVariable').trigger('click')
            }
        )
    }
});

// Deselect all for variables
$(".deselect-variable").click(function () {

    $('.variable-details tr').each( function () {
        $(this).remove()
        $('.static-header').hide()
        filterChangeMessage()
    })

    $('#variable-select').val('').trigger('chosen:updated');
});

// Add variable to details table
$('#addVariable').click(function () {
    $('.static-header').show();

    var variable = $("#variable-select").val();
    var variable_table = $('.variable-table-select')

    // // AJAX address to load variables into dropdown
    var target = "/get_variable_details/"+ variable + "/All/All";

    $.get({
        url: target,
        success: function (data) {

            // Get current selection
            var selected = []
            variable_table.each(function () {
            selected.push($(this).val())
            })

            addVariableRow(data);
            setVaribleTableHeader();

            // Reset rows to match previous selection
            variable_table.each(function (index, value) {
                var id = $(this).parent().attr('id')
                $('#' + id + ' option[value="'+ selected[index] + '"]').prop('selected', true)
            })
        }
    })

});


// Experiments Chosen box
var experimentChosen = $(".experiments").chosen({width:"95%",placeholder_text_multiple: "Start typing an experiment name or click to see the list"});

// Don't auto close the chosen box
keepChosenOpen(experimentChosen);

experimentChosen.change(function () {
    // Display message if the filter is changed.
    filterChangeMessage()
});

// Select all for experiments
$(".select-expr").click(function () {
    $('.experiments option').prop('selected', true);
    $('.experiments').trigger('chosen:updated')
});

// Deselect all for experiments
$(".deselect-expr").click(function () {
    $('.experiments option').prop('selected', false);
    $('.experiments').trigger('chosen:updated')
});

//Min Ensemble Size dropdown
$('#ensemble_size').change(function () {
    // Display message if the filter is changed.
    filterChangeMessage()
});


// AJAX request to get the page results.
$('#get_results').click(function () {
    var data_results = $("#results");

    // Make results semi transparent while loading to indicate loading.
    data_results.fadeTo('fast',0.5);

    // Get the data from the form
    var datastring = $("#filterform").serialize();
    var target = "/data-availability/";
    $.ajax({
        type: "POST",
        url: target,
        data: datastring,
        dataType: "json",
        success: function (returnedData) {
            var results = returnedData.results
            if (results.length < 1){
                // No results are returned, hide results table  and display message
                $('#results').hide()
                $('#messages').html("<h4 class='text-danger'>Your search returned no results, please edit your filters and try again.</h4>").fadeTo('fast',1)

            }
            else {
                // Handle results
                // Hide the "no results" message
                $('#messages').hide();
                $('#results h3').html('Results <span class="badge">'+ results.length +'</span>')

                var i;
                var results_string = "";
                var institute, model, experiment, ensembles;

                // build rows
                for (i = 0; i < results.length; i++) {
                    institute = "<td>" + results[i].institute + "</td>";
                    model = "<td>" + results[i].model + "</td>";
                    experiment = "<td>" + results[i].experiment + "</td>";
                    ensembles = "<td style='max-width: 40vw'>" + ensembleTags(results[i].ensembles) + "</td>"
                    results_string = results_string.concat("<tr>" + institute + model + experiment + ensembles + "</tr>")
                }

                // Push results to container element
                $("#data-availability-results").html(results_string);

                // show results table
                $('#results table').show();
                $('#results div:eq(1)').show();
                data_results.fadeTo('fast', 1)
            }

        },
        error: function (response) {
            // Handle errors

            // Hide results table
            $('#results').hide()

            // Display messages div
            $("#messages").fadeTo('fast',1)
            if (response.status === 400){
                // Show message
                $('#messages').html("<h4 class='text-danger'>Missing selection from one of the filters. Make sure you have made" +
                    " a selection for all filters and try again.</h4>").show()

            }
            else{
                $('#messages').html("<h4 class='text-danger'>Error retrieving results: "+ response.statusText +"</h4>").show()
            }
        }
    })
})


// Link the table and frequency columns in the variable detail table so that only valid options are submittable.
$(document).on('change','.variable-table-select',function (event) {
    // Display message if the filter is changed.
    filterChangeMessage()

    var id = $(this).parent().attr('id')
    var row_num = id.split("-")[0]
    var data_list = []
    var variable, table, freq, target, selector

    variable = $('#'+ row_num + '-variable input').val()
    table = $('#'+ row_num + '-table select').val()
    freq = $('#'+ row_num + '-frequency select').val()

    // set the target url to make use of the api
    if (id.indexOf('table') !== -1){
        selector = 'table'
        target = "/get_variable_details/" + variable + '/' + table + '/All'
    } else {
        selector = 'freq'
        target = "/get_variable_details/" + variable + '/All/' + freq
    }

    // AJAX request to get the results and modify the UI.
    $.get(target, function (data) {

        if (selector === 'table' && data.frequencies.length >= 1){
            $('#' + row_num + '-frequency select option[value="'+ data.frequencies[0] +'"]').prop('selected',true)
        }

        if (selector === 'freq' && data.tables.length >= 1){
            $('#' + row_num + '-table select option[value="'+ data.tables[0] +'"]').prop('selected',true)

        }
    })
})

// Reset all filters
$('#clear_filters').click(function () {
    $(".deselect-variable").trigger('click')
    $('.experiments option').prop('selected', false);
    $('.experiments').trigger('chosen:updated')
    $('#ensemble-size').val('1')
})

