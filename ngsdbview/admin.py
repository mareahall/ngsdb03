from django.contrib import admin
from django.contrib.auth.models import User
from django.contrib.auth.admin import UserAdmin
from ngsdbview.models import *

from django import forms
from django.forms.models import BaseInlineFormSet

from ngsdbview.autoregister import autoregister





class ChromosomeAdmin(admin.ModelAdmin):
    list_display = ('chromosome_id', 'chromosome_name', 'size', 'genome_name', 'genome_version')
admin.site.register(Chromosome, ChromosomeAdmin)

class Statistics_cvsAdmin(admin.ModelAdmin):
    list_display = ('cvterm_id', 'cvterm', 'cv_notes')
admin.site.register(Statistics_cv, Statistics_cvsAdmin)

class StatisticsAdmin(admin.ModelAdmin):
    list_display = ('stats_id', 'snp', 'stats_cvterm', 'cv_value')
admin.site.register(Statistics, StatisticsAdmin)

#______________________________________________________________________________


admin.site.unregister(User)
class UserProfileInline(admin.StackedInline):
    model = UserProfile

class UserProfileAdmin(UserAdmin):
    inlines = [ UserProfileInline, ]
admin.site.register(User, UserProfileAdmin)

#admin.site.register(Library)

class AuthorAdmin(admin.ModelAdmin):
    list_display = ('firstname', 'lastname', 'designation', 'email')
admin.site.register(Author, AuthorAdmin)

class CollaboratorAdmin(admin.ModelAdmin):
    list_display = ('firstname', 'lastname', 'affiliation', 'email')
admin.site.register(Collaborator, CollaboratorAdmin)

class FeatureAdmin(admin.ModelAdmin):
    list_display = ('genome', 'geneid', 'geneproduct', 'annotation')
    search_fields = ['geneid', 'geneproduct', 'annotation']
    list_filter = ['genome']
admin.site.register(Feature, FeatureAdmin)

class GenomeAdmin(admin.ModelAdmin):
    list_display = ('genome_id', 'organism', 'version', 'source')
admin.site.register(Genome, GenomeAdmin)

class OrganismAdmin(admin.ModelAdmin):
    list_display = ('organismcode', 'genus', 'speceis', 'strain', 'source')
admin.site.register(Organism, OrganismAdmin)

class SoftwareAdmin(admin.ModelAdmin):
    list_display = ('software_id', 'name', 'version', 'algorithm', 'source', 'sourceuri')
admin.site.register(Software, SoftwareAdmin)


# code for editing Libraryfile & Libraryprop while in Library admin page
class LibraryfileInline(admin.TabularInline):
    model = Libraryfile

class LibrarypropInline(admin.TabularInline):
    model = Libraryprop

class LibraryAdmin(admin.ModelAdmin):
    inlines = [ LibraryfileInline, LibrarypropInline ]
admin.site.register(Library, LibraryAdmin)


# code for editing Resultfile/Resultprop while in Library admin page
class ResultfileInline(admin.TabularInline):
    model = Resultfile
class ResultpropInline(admin.TabularInline):
    model = Resultprop

class ResultAdmin(admin.ModelAdmin):
    inlines = [ ResultfileInline, ResultpropInline ]
admin.site.register(Result, ResultAdmin)


# code for editing Analysisfile/Analyisprop while in Library admin page
class AnalysisfileInline(admin.TabularInline):
    model = Analysisfile

class AnalysispropInline(admin.TabularInline):
    model = Analysisprop

class AnalysisAdmin(admin.ModelAdmin):
    inlines = [ AnalysisfileInline, AnalysispropInline ]
admin.site.register(Analysis, AnalysisAdmin)

autoregister('ngsdbview')
