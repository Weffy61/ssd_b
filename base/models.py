from django.contrib.postgres.indexes import GinIndex
from django.db import models


class Person(models.Model):
    # Indexes was added CONCURRENTLY handdled
    first_name = models.CharField(max_length=100, null=True)
    last_name = models.CharField(max_length=100, null=True)
    middle_name = models.CharField(max_length=100, blank=True, null=True)
    home_addresses = models.ManyToManyField('PersonAddress', related_name='persons')
    ssn = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        ordering = ['id', 'first_name', 'last_name']
        unique_together = ('first_name', 'last_name', 'middle_name', 'ssn')
        indexes = [
            models.Index(fields=['first_name', 'last_name', 'middle_name', 'ssn']),
            GinIndex(fields=["first_name"], name="trgm_idx_first_name", opclasses=["gin_trgm_ops"]),
            GinIndex(fields=["last_name"], name="trgm_idx_last_name", opclasses=["gin_trgm_ops"]),
        ]
        verbose_name = 'Person'
        verbose_name_plural = 'Persons'

    def __str__(self):
        return f'{self.first_name} {self.middle_name or ""} {self.last_name}'


class PersonalData(models.Model):
    dob = models.DateField(blank=True, null=True)
    name_suffix = models.CharField(max_length=100, blank=True, null=True)
    alt1_dob = models.DateField(blank=True, null=True)
    alt2_dob = models.DateField(blank=True, null=True)
    alt3_dob = models.DateField(blank=True, null=True)
    aka1_fullname = models.CharField(max_length=200, blank=True, null=True)
    aka2_fullname = models.CharField(max_length=200, blank=True, null=True)
    aka3_fullname = models.CharField(max_length=200, blank=True, null=True)
    person = models.ForeignKey(Person, on_delete=models.CASCADE, related_name='personal_datas')

    class Meta:
        verbose_name = 'Personal entry'
        verbose_name_plural = 'Personal data'

    def __str__(self):
        return f'{self.person.first_name} {self.person.middle_name} {self.person.last_name}'


class PersonAddress(models.Model):
    # Indexes was added CONCURRENTLY handdled
    address = models.TextField(blank=True, null=True)
    city = models.CharField(max_length=100, blank=True, null=True)
    county = models.CharField(max_length=100, blank=True, null=True)
    state = models.CharField(max_length=100, blank=True, null=True)
    zip_code = models.CharField(max_length=50, blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)

    class Meta:
        ordering = ['id']
        unique_together = ('address', 'city', 'county', 'state', 'zip_code', 'phone')
        verbose_name = 'Personal address'
        verbose_name_plural = 'Personal addresses'

    def __str__(self):
        return f'{self.address}, {self.city}, {self.state}, {self.zip_code}'


class AllPersonsData(models.Model):
    first_name = models.CharField(max_length=100, null=True)
    last_name = models.CharField(max_length=100, null=True)
    middle_name = models.CharField(max_length=100, blank=True, null=True)
    ssn = models.CharField(max_length=10, blank=True, null=True)
    dob = models.DateField(blank=True, null=True)
    name_suffix = models.CharField(max_length=100, blank=True, null=True)
    alt1_dob = models.DateField(blank=True, null=True)
    alt2_dob = models.DateField(blank=True, null=True)
    alt3_dob = models.DateField(blank=True, null=True)
    aka1_fullname = models.CharField(max_length=200, blank=True, null=True)
    aka2_fullname = models.CharField(max_length=200, blank=True, null=True)
    aka3_fullname = models.CharField(max_length=200, blank=True, null=True)
    address = models.TextField(blank=True, null=True)
    city = models.CharField(max_length=100, blank=True, null=True)
    county = models.CharField(max_length=100, blank=True, null=True)
    state = models.CharField(max_length=100, blank=True, null=True)
    zip_code = models.CharField(max_length=50, blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)
    start_date = models.DateField(blank=True, null=True)

    class Meta:
        indexes = [
            models.Index(fields=['first_name', 'last_name', 'middle_name', 'ssn']),
        ]
