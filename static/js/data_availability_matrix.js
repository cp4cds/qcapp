/**
 * Created by vdn73631 on 23/11/2017.
 */
$(window).resize(setVaribleTableHeader())

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

// -------------------------------------------------- Main Code --------------------------------------------------------


// Variables Chosen box
$(".variables").chosen({width: "95%",placeholder_text_multiple: "Choose one or more variables"}).change(function () {
    var data = {};
    var variables = $(".variables").val();
    data["variables"] = JSON.stringify(variables);

    // AJAX address to load variables into dropdown
    var target = "/data-availability-variables";

    // Only show the table if there is content to display
    if (variables.length > 0){
        $(".static-header").show()
    }
    else {
        $(".static-header").hide()
    }

    // POST request to retrieve variable tables and frequencies.
    $.ajax({
        method: "POST",
        url: target,
        data: data,
        success: function (returned_data) {
            var i, variable_details = "<tr>", vari;
            for (i = 0; i < returned_data.variables.length; i++) {
                vari = returned_data.variables[i];

                var table_select = buildSelect(vari, "tables")
                var freq_select = buildSelect(vari, "freqs")

                variable_details = variable_details.concat("<td id='"+ i +"-variable'>" + vari.variable + "</td><td id='"+ i +"-table'>" + table_select + "</td><td id='" + i + "-frequency'>" + freq_select + "</td>")
                variable_details = variable_details.concat("</tr>")
            }

            // Push content to container on page
            $(".variable-details").html(variable_details)

            // Set the headers to match the content
            setVaribleTableHeader()


        },
        headers: {
            'X-CSRFTOKEN': getCookie('csrftoken')
        },
        dataType: "json"
    })
});

$(".select-variable").click(function () {
    $('.variables option').prop('selected', true);
    $('.variables').trigger('chosen:updated').trigger('change')
});
$(".deselect-variable").click(function () {
    $('.variables option').prop('selected', false);
    $('.variables').trigger('chosen:updated').trigger('change')
});


// Experiments Chosen box
$(".experiments").chosen({width:"95%",placeholder_text_multiple: "Choose one or more experiments"});

$(".select-expr").click(function () {
    $('.experiments option').prop('selected', true);
    $('.experiments').trigger('chosen:updated')
});

$(".deselect-expr").click(function () {
    $('.experiments option').prop('selected', false);
    $('.experiments').trigger('chosen:updated')
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
                $('#results table').hide();
                $('#results div:eq(1)').hide();
                data_results.fadeTo('fast', 1);
                $('#results div:first').html("<h4 class='text-danger'>Your search returned no results, please edit your filters and try again.</h4>").show()

            }
            else {
                // Handle results
                // Hide the "no results" message
                $('#results div:first').hide();
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
            $('#results table').hide()
            $('#results div:eq(1)').hide();
            // Display results div
            $("#results").fadeTo('fast',1)
            if (response.status === 400){
                // Show message
                $('#results div:first').html("<h4 class='text-danger'>Missing selection from one of the filters. Make sure you have made" +
                    " a selection for all filters and try again.</h4>").show()

            }
            else{
                $('#results div:first').html("<h4 class='text-danger'>Error retrieving results: "+ response.statusText +"</h4>").show()
            }
        }
    })
})


// Link the table and frequency columns in the variable detail table so that only valid options are submittable.
$('.variable-details').on('change','.variable-table-select',function (event) {
    var id = $(this).parent().attr('id')
    var row_num = id.split("-")[0]
    var data_list = []
    var variable, table, freq, target, selector

    variable = $('#'+ row_num + '-variable').html()
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

$('#clear_filters').click(function () {
    $('.variables option').prop('selected', false);
    $('.variables').trigger('chosen:updated').trigger('change')
    $('.experiments option').prop('selected', false);
    $('.experiments').trigger('chosen:updated')
    $('#ensemble-size').val('1')
})

