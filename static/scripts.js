Dropzone.options.uploadForm = {
    success: function(file, response) {

        var info = (response.info) ? response.info : '';
        document.getElementById('info').innerHTML = info;

        var download = (response.download) ? '<a class="button" href="' + response.download + '">Download filtered SDMX data</a>' : '';
        document.getElementById('download').innerHTML = download;

        var summary = '';
        if (response.dsd_violations > 0 || response.content_violations > 0) {
            summary += '<h2>Results:</h2>';
            if (response.dsd_violations > 0) {
                summary += '<p>' + response.dsd_violations + ' series keys had DSD violations (out of ' + response.total + ') and were removed.</p>';
            }
            if (response.content_violations > 0) {
                summary += '<p>Of the remaining ' + response.series + ' series keys, ' + response.content_violations + ' had content violations. <em>These will need to be fixed before output can be generated.</em></p>';
            }
        }
        document.getElementById('summary').innerHTML = summary;

        var content_messages = '';
        if (response.content_messages && response.content_messages.length > 0) {
            content_messages += '<h2>Content violations (must be fixed):</h2>';
            content_messages += '<ol>';
            response.content_messages.forEach(function(message) {
                content_messages += '<li>' + message + '</li>';
            });
            content_messages += '</ol>';
        }
        document.getElementById('content-messages').innerHTML = content_messages;

        var dsd_messages = '';
        if (response.dsd_messages && response.dsd_messages.length > 0) {
            dsd_messages += '<h2>DSD violations (automatically removed):</h2>';
            dsd_messages += '<ol>';
            response.dsd_messages.forEach(function(message) {
                dsd_messages += '<li>' + message + '</li>';
            });
            dsd_messages += '</ol>';
        }
        document.getElementById('dsd-messages').innerHTML = dsd_messages;
    }
}