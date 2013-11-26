from django.conf.urls import patterns, include, url

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()


urlpatterns = patterns('',
    (r'^grappelli/', include('grappelli.urls')), # grappelli URLS
    # Examples:
    # url(r'^$', 'ngsdb03.views.home', name='home'),
    # url(r'^ngsdb03/', include('ngsdb03.foo.urls')),
    url(r'^ngsdbview/', include('ngsdbview.urls')),
    url(r'^samples/', include('samples.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    url(r'^admin/', include(admin.site.urls)),
    url(r'^$', 'ngsdbview.views02.Dashboard'),
)
