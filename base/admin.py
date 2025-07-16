from django.contrib import admin

from base.models import Person, PersonalData, PersonAddress, AllPersonsData


class PersonAddressInline(admin.TabularInline):
    model = Person.home_addresses.through
    extra = 0


class PersonalDataInline(admin.TabularInline):
    model = PersonalData
    extra = 0


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ['id', 'first_name', 'middle_name', 'last_name']
    raw_id_fields = ['home_addresses']
    search_fields = ['first_name', 'last_name', 'ssn']
    inlines = [PersonAddressInline, PersonalDataInline]


@admin.register(PersonAddress)
class PersonAddressAdmin(admin.ModelAdmin):
    list_display = ['id', 'address', 'city', 'state', 'zip_code']
    search_fields = ['address', 'city', 'state', 'zip_code']


@admin.register(AllPersonsData)
class AllPersonsDataAdmin(admin.ModelAdmin):
    pass
