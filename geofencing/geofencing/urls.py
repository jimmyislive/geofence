from django.conf.urls import patterns, include, url
from django.conf import settings
from django.conf.urls.static import static

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    # Examples:

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
    url(r'^$', 'geofencing.dispatch.views.index'),
    url(r'^trips/', 'geofencing.dispatch.views.trips'),
    url(r'^query/trip_count_right_now/', 'geofencing.dispatch.views.current_trip_count'),
    url(r'^query/trip_count_at_time_t/', 'geofencing.dispatch.views.time_t_trip_count'),
    url(r'^query/trips_passed_through/', 'geofencing.dispatch.views.trips_passed_through'),
    url(r'^query/trips_start_stop/', 'geofencing.dispatch.views.trips_start_stop'),
) + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
