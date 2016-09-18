from __future__ import unicode_literals

from django.db import models

# Create your models here.


class A(models.Model):
    a_char = models.CharField(max_length=244)
    a_int = models.IntegerField()

    def __str__(self):
        return self.a_char


class B(models.Model):
    b_char = models.CharField(max_length=244)
    b_int = models.IntegerField()
    b_for = models.ForeignKey(A)

    def __str__(self):
        return self.b_char + " " + self.b_for.a_char


class C(models.Model):
    c_char = models.CharField(max_length=244)
    c_int = models.IntegerField()
    c_for = models.ForeignKey(B)

    def __str__(self):
        return self.c_char


